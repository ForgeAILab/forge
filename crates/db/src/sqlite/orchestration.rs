use super::*;
use crate::*;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteRow, Row};
use std::collections::BTreeMap;

const PROJECT_AGENT_PERMISSION_CEILING: &str = r#"{"allowed":["read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}"#;
const PROJECT_OPERATING_SKILL_KEY: &str = "forge.project.orchestration/v1";

fn orchestration_scoped_idempotency_key(
    operation: &str,
    scope_id: &str,
    principal_id: &str,
    client_key: &str,
) -> String {
    format!(
        "forge-idem-v1:{}:{}:{}:{client_key}",
        hex::encode(operation),
        hex::encode(scope_id),
        hex::encode(principal_id),
    )
}

fn sha256_hex(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn profile_policy_digest(tool_policy_json: &str) -> String {
    let mut bytes = Vec::with_capacity(32 + tool_policy_json.len());
    bytes.extend_from_slice(b"forge.project-agent-policy/v1\0");
    bytes.extend_from_slice(tool_policy_json.as_bytes());
    sha256_hex(&bytes)
}

fn orchestration_write_error(error: sqlx::Error) -> DbError {
    if let sqlx::Error::Database(database_error) = &error {
        let message = database_error.message().to_ascii_lowercase();
        if message.contains("unique constraint") || message.contains("constraint failed") {
            return DbError::VersionConflict;
        }
    }
    check_error(error)
}

fn required_string(row: &SqliteRow, column: &str) -> Result<String> {
    row.try_get(column).map_err(DbError::from)
}

fn optional_string(row: &SqliteRow, column: &str) -> Result<Option<String>> {
    row.try_get(column).map_err(DbError::from)
}

fn json_string<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(serde_json::Value::as_str)
}

/// Return the caller's handoff request after removing values that this
/// transaction fills in from durable rows.  The fingerprint is deliberately
/// independent of the target Chat UUID and delivery timestamp allocated by
/// the transaction, so a lost-response replay can submit the original packet
/// without pretending those runtime values were caller authority.
fn normalize_handoff_request(value: &serde_json::Value) -> Result<serde_json::Value> {
    let mut normalized = value.clone();
    let object = normalized.as_object_mut().ok_or_else(|| {
        DbError::Check("handoff source_revisions_json must be a JSON object".to_owned())
    })?;
    object.remove("approval_id");
    if let Some(request) = object
        .get_mut("request")
        .and_then(serde_json::Value::as_object_mut)
    {
        // These values are canonicalized from typed DB input below. Preserve
        // every other caller-supplied request field so an altered replay
        // cannot hide behind an ignored JSON subtree.
        request.remove("policy_revision");
        request.remove("policy_digest");
        request.remove("source_revisions_digest");
        request.remove("authorization");
    }
    if let Some(target) = object
        .get_mut("target")
        .and_then(serde_json::Value::as_object_mut)
    {
        target.insert("chat_id".to_owned(), serde_json::Value::Null);
    }
    if let Some(delivery) = object
        .get_mut("delivery")
        .and_then(serde_json::Value::as_object_mut)
    {
        delivery.insert("delivered_at".to_owned(), serde_json::Value::Null);
    }
    if let Some(source) = object
        .get_mut("source")
        .and_then(serde_json::Value::as_object_mut)
    {
        // The source message id is persisted in agent_handoff.source_message_id
        // and is added to the immutable packet by this transaction.
        source.remove("message_id");
    }
    Ok(normalized)
}

fn handoff_request_fingerprint(
    value: &serde_json::Value,
    input: &CreateProjectFromCharterApproval,
) -> Result<String> {
    let mut normalized = normalize_handoff_request(value)?;
    let object = normalized.as_object_mut().ok_or_else(|| {
        DbError::Check("handoff source_revisions_json must be a JSON object".to_owned())
    })?;
    // The create authorization is part of the immutable request identity, but
    // it is supplied as a typed input rather than trusted from handoff prose.
    // Include it in the normalized digest so a replay under a different
    // principal, action, event, or timestamp cannot reuse the receipt.
    let request = object
        .entry("request".to_owned())
        .or_insert_with(|| serde_json::json!({}));
    let request = request
        .as_object_mut()
        .ok_or_else(|| DbError::Check("handoff request must be a JSON object".to_owned()))?;
    let source_revisions_json = canonical_json_string(&input.source_revisions_json)?;
    request.insert(
        "source_revisions_json".to_owned(),
        serde_json::Value::String(source_revisions_json),
    );
    request.insert(
        "authorization".to_owned(),
        serde_json::json!({
            "principal_type": input.create_principal_type,
            "principal_id": input.create_principal_id,
            "authorization_basis": input.create_authorization_basis,
            "action": input.create_action,
            "event_id": input.create_event_id,
            "occurred_at": input.create_occurred_at,
        }),
    );
    let canonical = canonicalize_json_value(&normalized);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| DbError::Check(format!("invalid handoff request: {error}")))?;
    Ok(sha256_hex(&bytes))
}

fn canonical_json_string(value: &str) -> Result<String> {
    let parsed: serde_json::Value = serde_json::from_str(value)
        .map_err(|error| DbError::Check(format!("invalid handoff source manifest: {error}")))?;
    serde_json::to_string(&canonicalize_json_value(&parsed))
        .map_err(|error| DbError::Check(format!("invalid handoff source manifest: {error}")))
}

fn canonicalize_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let sorted = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_json_value(value)))
                .collect::<BTreeMap<_, _>>();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(canonicalize_json_value)
                .collect::<Vec<_>>(),
        ),
        scalar => scalar.clone(),
    }
}

fn valid_authorization_timestamp(value: &str) -> bool {
    // The timestamp is immutable user-provided evidence.  Validate its
    // representation here, but do not compare it with this machine's clock:
    // historical imports, delayed/replayed requests, and clock skew must not
    // make an otherwise exact receipt unreadable or mutable.
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

fn map_charter(row: SqliteRow) -> Result<ProjectCharterRecord> {
    Ok(ProjectCharterRecord {
        id: required_string(&row, "id")?,
        account_id: required_string(&row, "account_id")?,
        genesis_session_id: optional_string(&row, "genesis_session_id")?,
        project_id: optional_string(&row, "project_id")?,
        current_draft_revision_id: optional_string(&row, "current_draft_revision_id")?,
        current_approved_revision_id: optional_string(&row, "current_approved_revision_id")?,
        project_mode: required_string(&row, "project_mode")?,
        maturity: required_string(&row, "maturity")?,
        lifecycle: required_string(&row, "lifecycle")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_charter_revision(row: SqliteRow) -> Result<ProjectCharterRevisionRecord> {
    Ok(ProjectCharterRevisionRecord {
        id: required_string(&row, "id")?,
        charter_id: required_string(&row, "charter_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: optional_string(&row, "base_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        schema_version: required_string(&row, "schema_version")?,
        render_version: required_string(&row, "render_version")?,
        content_json: required_string(&row, "content_json")?,
        rendered_view: required_string(&row, "rendered_view")?,
        change_summary: required_string(&row, "change_summary")?,
        author_type: required_string(&row, "author_type")?,
        author_id: optional_string(&row, "author_id")?,
        source_message_id: optional_string(&row, "source_message_id")?,
        source_turn_job_id: optional_string(&row, "source_turn_job_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_charter_approval(row: SqliteRow) -> Result<ProjectCharterApprovalRecord> {
    Ok(ProjectCharterApprovalRecord {
        id: required_string(&row, "id")?,
        approval_type: required_string(&row, "approval_type")?,
        charter_id: required_string(&row, "charter_id")?,
        revision_id: required_string(&row, "revision_id")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        expected_charter_version: row.try_get("expected_charter_version")?,
        approved_name: optional_string(&row, "approved_name")?,
        approved_slug: optional_string(&row, "approved_slug")?,
        approved_project_mode: required_string(&row, "approved_project_mode")?,
        selected_identity_id: optional_string(&row, "selected_identity_id")?,
        selected_profile_id: optional_string(&row, "selected_profile_id")?,
        selected_operating_skill_revision_id: optional_string(
            &row,
            "selected_operating_skill_revision_id",
        )?,
        selected_policy_revision: optional_string(&row, "selected_policy_revision")?,
        selected_policy_digest: optional_string(&row, "selected_policy_digest")?,
        approving_principal_type: required_string(&row, "approving_principal_type")?,
        approving_principal_id: required_string(&row, "approving_principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        source_action: required_string(&row, "source_action")?,
        approval_event_id: optional_string(&row, "approval_event_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        consumed_project_id: optional_string(&row, "consumed_project_id")?,
        consumed_at: optional_string(&row, "consumed_at")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_canonical_conflict(row: SqliteRow) -> Result<ProjectCanonicalConflictRecord> {
    Ok(ProjectCanonicalConflictRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        domain: required_string(&row, "domain")?,
        governing_record_type: required_string(&row, "governing_record_type")?,
        governing_record_id: required_string(&row, "governing_record_id")?,
        governing_record_revision: required_string(&row, "governing_record_revision")?,
        governing_record_digest: required_string(&row, "governing_record_digest")?,
        conflicting_record_type: required_string(&row, "conflicting_record_type")?,
        conflicting_record_id: required_string(&row, "conflicting_record_id")?,
        conflicting_record_revision: required_string(&row, "conflicting_record_revision")?,
        conflicting_record_digest: required_string(&row, "conflicting_record_digest")?,
        affected_paths_json: required_string(&row, "affected_paths_json")?,
        conflict_code: required_string(&row, "conflict_code")?,
        description: required_string(&row, "description")?,
        detected_by_type: required_string(&row, "detected_by_type")?,
        detected_by_id: optional_string(&row, "detected_by_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_reconciliation(row: SqliteRow) -> Result<ProjectReconciliationRecord> {
    Ok(ProjectReconciliationRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        conflict_id: required_string(&row, "conflict_id")?,
        record_type: required_string(&row, "record_type")?,
        record_id: required_string(&row, "record_id")?,
        record_revision: required_string(&row, "record_revision")?,
        record_digest: required_string(&row, "record_digest")?,
        governing_record_type: required_string(&row, "governing_record_type")?,
        governing_record_id: required_string(&row, "governing_record_id")?,
        governing_record_revision: required_string(&row, "governing_record_revision")?,
        governing_record_digest: required_string(&row, "governing_record_digest")?,
        state: required_string(&row, "state")?,
        current_resolution_id: optional_string(&row, "current_resolution_id")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_document(row: SqliteRow) -> Result<ProjectDocumentRecord> {
    Ok(ProjectDocumentRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        kind: required_string(&row, "kind")?,
        title: required_string(&row, "title")?,
        lifecycle: required_string(&row, "lifecycle")?,
        approval_policy: required_string(&row, "approval_policy")?,
        current_draft_revision_id: optional_string(&row, "current_draft_revision_id")?,
        current_approved_revision_id: optional_string(&row, "current_approved_revision_id")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_document_revision(row: SqliteRow) -> Result<ProjectDocumentRevisionRecord> {
    Ok(ProjectDocumentRevisionRecord {
        id: required_string(&row, "id")?,
        document_id: required_string(&row, "document_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: optional_string(&row, "base_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        schema_version: required_string(&row, "schema_version")?,
        render_version: required_string(&row, "render_version")?,
        content_json: required_string(&row, "content_json")?,
        rendered_view: required_string(&row, "rendered_view")?,
        change_summary: required_string(&row, "change_summary")?,
        author_type: required_string(&row, "author_type")?,
        author_id: optional_string(&row, "author_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_document_approval(row: SqliteRow) -> Result<ProjectDocumentApprovalRecord> {
    Ok(ProjectDocumentApprovalRecord {
        id: required_string(&row, "id")?,
        document_id: required_string(&row, "document_id")?,
        revision_id: required_string(&row, "revision_id")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        lifecycle: required_string(&row, "lifecycle")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_decision_candidate(row: SqliteRow) -> Result<ProjectDecisionCandidateRecord> {
    Ok(ProjectDecisionCandidateRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        question: required_string(&row, "question")?,
        context_json: required_string(&row, "context_json")?,
        options_json: required_string(&row, "options_json")?,
        selected_outcome: optional_string(&row, "selected_outcome")?,
        rationale: optional_string(&row, "rationale")?,
        principal_type: optional_string(&row, "principal_type")?,
        principal_id: optional_string(&row, "principal_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        expected_project_version: row.try_get("expected_project_version")?,
        effective_decision_id: optional_string(&row, "effective_decision_id")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_decision(row: SqliteRow) -> Result<ProjectDecisionRecord> {
    Ok(ProjectDecisionRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        state: required_string(&row, "state")?,
        decision_class: required_string(&row, "decision_class")?,
        question: required_string(&row, "question")?,
        context_json: required_string(&row, "context_json")?,
        options_json: required_string(&row, "options_json")?,
        selected_outcome: required_string(&row, "selected_outcome")?,
        rationale: required_string(&row, "rationale")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authority_basis: required_string(&row, "authority_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        charter_revision_id: optional_string(&row, "charter_revision_id")?,
        baseline_revision_id: optional_string(&row, "baseline_revision_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        affected_records_json: required_string(&row, "affected_records_json")?,
        supersedes_decision_id: optional_string(&row, "supersedes_decision_id")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_baseline(row: SqliteRow) -> Result<ProjectExecutionBaselineRecord> {
    Ok(ProjectExecutionBaselineRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        current_revision_id: optional_string(&row, "current_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_baseline_revision(row: SqliteRow) -> Result<ProjectExecutionBaselineRevisionRecord> {
    Ok(ProjectExecutionBaselineRevisionRecord {
        id: required_string(&row, "id")?,
        baseline_id: required_string(&row, "baseline_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: optional_string(&row, "base_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        charter_revision_id: required_string(&row, "charter_revision_id")?,
        document_revisions_json: required_string(&row, "document_revisions_json")?,
        plan_items_json: required_string(&row, "plan_items_json")?,
        milestone_id: optional_string(&row, "milestone_id")?,
        milestone_ids_json: required_string(&row, "milestone_ids_json")?,
        milestone_definition_revision_ids_json: required_string(
            &row,
            "milestone_definition_revision_ids_json",
        )?,
        primary_milestone_id: optional_string(&row, "primary_milestone_id")?,
        release_policy_json: required_string(&row, "release_policy_json")?,
        release_policy_revision: required_string(&row, "release_policy_revision")?,
        release_policy_digest: required_string(&row, "release_policy_digest")?,
        acceptance_matrix_json: required_string(&row, "acceptance_matrix_json")?,
        capability_classes_json: required_string(&row, "capability_classes_json")?,
        risk_classes_json: required_string(&row, "risk_classes_json")?,
        adaptive_envelope_json: required_string(&row, "adaptive_envelope_json")?,
        elevated_operations_json: required_string(&row, "elevated_operations_json")?,
        exclusions_json: required_string(&row, "exclusions_json")?,
        rollback_recovery_json: required_string(&row, "rollback_recovery_json")?,
        schema_version: required_string(&row, "schema_version")?,
        render_version: required_string(&row, "render_version")?,
        rendered_view: required_string(&row, "rendered_view")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_baseline_approval(row: SqliteRow) -> Result<ProjectExecutionBaselineApprovalRecord> {
    Ok(ProjectExecutionBaselineApprovalRecord {
        id: required_string(&row, "id")?,
        baseline_id: required_string(&row, "baseline_id")?,
        revision_id: required_string(&row, "revision_id")?,
        expected_project_version: row.try_get("expected_project_version")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        explicit_event: required_string(&row, "explicit_event")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        lifecycle: required_string(&row, "lifecycle")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_milestone(row: SqliteRow) -> Result<ProjectMilestoneRecord> {
    Ok(ProjectMilestoneRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_sequence: row.try_get("milestone_sequence")?,
        milestone_key: required_string(&row, "milestone_key")?,
        display_label: optional_string(&row, "display_label")?,
        current_definition_revision_id: optional_string(&row, "current_definition_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        blocker_reason_json: required_string(&row, "blocker_reason_json")?,
        stale_reason_json: required_string(&row, "stale_reason_json")?,
        reconciliation_reason_json: required_string(&row, "reconciliation_reason_json")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_milestone_revision(row: SqliteRow) -> Result<ProjectMilestoneRevisionRecord> {
    Ok(ProjectMilestoneRevisionRecord {
        id: required_string(&row, "id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: optional_string(&row, "base_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        display_label: optional_string(&row, "display_label")?,
        outcome: required_string(&row, "outcome")?,
        included_scope_json: required_string(&row, "included_scope_json")?,
        excluded_scope_json: required_string(&row, "excluded_scope_json")?,
        charter_revision_id: optional_string(&row, "charter_revision_id")?,
        document_revisions_json: required_string(&row, "document_revisions_json")?,
        task_selection_json: required_string(&row, "task_selection_json")?,
        dependencies_json: required_string(&row, "dependencies_json")?,
        risks_json: required_string(&row, "risks_json")?,
        acceptance_checks_json: required_string(&row, "acceptance_checks_json")?,
        evidence_requirements_json: required_string(&row, "evidence_requirements_json")?,
        known_issues_json: required_string(&row, "known_issues_json")?,
        change_summary: required_string(&row, "change_summary")?,
        schema_version: required_string(&row, "schema_version")?,
        render_version: required_string(&row, "render_version")?,
        rendered_view: required_string(&row, "rendered_view")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        author_type: required_string(&row, "author_type")?,
        author_id: optional_string(&row, "author_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_milestone_check(row: SqliteRow) -> Result<ProjectMilestoneCheckRecord> {
    Ok(ProjectMilestoneCheckRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        definition_revision_id: required_string(&row, "definition_revision_id")?,
        check_key: required_string(&row, "check_key")?,
        description: required_string(&row, "description")?,
        required: row.try_get::<i64, _>("required")? != 0,
        source_kind: required_string(&row, "source_kind")?,
        expected_result: required_string(&row, "expected_result")?,
        evidence_required: row.try_get::<i64, _>("evidence_required")? != 0,
        version: row.try_get("version")?,
        current_result_id: optional_string(&row, "current_result_id")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_milestone_result(row: SqliteRow) -> Result<ProjectMilestoneCheckResultRecord> {
    Ok(ProjectMilestoneCheckResultRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        check_id: required_string(&row, "check_id")?,
        definition_revision_id: required_string(&row, "definition_revision_id")?,
        outcome: required_string(&row, "outcome")?,
        source_kind: required_string(&row, "source_kind")?,
        source_manifest_json: required_string(&row, "source_manifest_json")?,
        input_digest: required_string(&row, "input_digest")?,
        governing_charter_revision_id: optional_string(&row, "governing_charter_revision_id")?,
        governing_baseline_revision_id: optional_string(&row, "governing_baseline_revision_id")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        expected_version: row.try_get("expected_version")?,
        explicit_event: required_string(&row, "explicit_event")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_readiness(row: SqliteRow) -> Result<ProjectReadinessSnapshotRecord> {
    Ok(ProjectReadinessSnapshotRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        definition_revision_id: required_string(&row, "definition_revision_id")?,
        baseline_id: required_string(&row, "baseline_id")?,
        baseline_revision_id: required_string(&row, "baseline_revision_id")?,
        baseline_digest: required_string(&row, "baseline_digest")?,
        release_policy_revision: required_string(&row, "release_policy_revision")?,
        release_policy_digest: required_string(&row, "release_policy_digest")?,
        input_manifest_json: required_string(&row, "input_manifest_json")?,
        event_watermark: required_string(&row, "event_watermark")?,
        outcome: required_string(&row, "outcome")?,
        blocking_reasons_json: required_string(&row, "blocking_reasons_json")?,
        check_results_json: required_string(&row, "check_results_json")?,
        waiver_manifest_json: required_string(&row, "waiver_manifest_json")?,
        evidence_manifest_json: required_string(&row, "evidence_manifest_json")?,
        commit_context_json: required_string(&row, "commit_context_json")?,
        computing_policy_revision: required_string(&row, "computing_policy_revision")?,
        readiness_digest: required_string(&row, "readiness_digest")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        expected_milestone_version: row.try_get("expected_milestone_version")?,
        explicit_event: required_string(&row, "explicit_event")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_release(row: SqliteRow) -> Result<ProjectReleaseRecord> {
    Ok(ProjectReleaseRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        release_sequence: row.try_get("release_sequence")?,
        release_revision: row.try_get("release_revision")?,
        release_identifier: required_string(&row, "release_identifier")?,
        milestone_revision_id: required_string(&row, "milestone_revision_id")?,
        readiness_snapshot_id: required_string(&row, "readiness_snapshot_id")?,
        readiness_digest: required_string(&row, "readiness_digest")?,
        baseline_id: required_string(&row, "baseline_id")?,
        baseline_revision_id: required_string(&row, "baseline_revision_id")?,
        baseline_digest: required_string(&row, "baseline_digest")?,
        release_policy_revision: required_string(&row, "release_policy_revision")?,
        release_policy_digest: required_string(&row, "release_policy_digest")?,
        summary: required_string(&row, "summary")?,
        changelog: required_string(&row, "changelog")?,
        known_issues_json: required_string(&row, "known_issues_json")?,
        charter_revision_id: optional_string(&row, "charter_revision_id")?,
        document_revisions_json: required_string(&row, "document_revisions_json")?,
        decision_ids_json: required_string(&row, "decision_ids_json")?,
        task_references_json: required_string(&row, "task_references_json")?,
        validation_references_json: required_string(&row, "validation_references_json")?,
        git_references_json: required_string(&row, "git_references_json")?,
        evidence_references_json: required_string(&row, "evidence_references_json")?,
        waivers_json: required_string(&row, "waivers_json")?,
        releasing_principal_type: required_string(&row, "releasing_principal_type")?,
        releasing_principal_id: required_string(&row, "releasing_principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        explicit_event: required_string(&row, "explicit_event")?,
        schema_version: required_string(&row, "schema_version")?,
        snapshot_digest: required_string(&row, "snapshot_digest")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_release_reference(row: SqliteRow) -> Result<ProjectReleaseReferenceRecord> {
    Ok(ProjectReleaseReferenceRecord {
        release_id: required_string(&row, "release_id")?,
        ordinal: row.try_get("ordinal")?,
        reference_kind: required_string(&row, "reference_kind")?,
        record_id: required_string(&row, "record_id")?,
        record_version: optional_string(&row, "record_version")?,
        record_state: optional_string(&row, "record_state")?,
        record_digest: optional_string(&row, "record_digest")?,
        metadata_json: required_string(&row, "metadata_json")?,
    })
}

async fn select_one<T, F>(
    query: &str,
    pool: &SqlitePool,
    bind: &str,
    mapper: F,
) -> Result<Option<T>>
where
    F: FnOnce(SqliteRow) -> Result<T>,
{
    sqlx::query(query)
        .bind(bind)
        .fetch_optional(pool)
        .await?
        .map(mapper)
        .transpose()
}

#[async_trait]
impl ProjectOrchestrationRepo for SqliteDb {
    async fn get_project_charter(&self, id: &str) -> Result<Option<ProjectCharterRecord>> {
        select_one(
            "SELECT * FROM project_charter WHERE id = ?",
            self.pool(),
            id,
            map_charter,
        )
        .await
    }

    async fn get_project_adoption_charter(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectCharterRecord>> {
        select_one(
            "SELECT * FROM project_charter
             WHERE project_id = ? AND genesis_session_id IS NULL",
            self.pool(),
            project_id,
            map_charter,
        )
        .await
    }

    async fn get_project_charter_for_account(
        &self,
        id: &str,
        account_id: &str,
    ) -> Result<Option<ProjectCharterRecord>> {
        sqlx::query("SELECT * FROM project_charter WHERE id = ? AND account_id = ?")
            .bind(id)
            .bind(account_id)
            .fetch_optional(self.pool())
            .await?
            .map(map_charter)
            .transpose()
    }

    async fn create_project_charter(
        &self,
        input: CreateProjectCharter,
    ) -> Result<ProjectCharterRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
            let genesis = sqlx::query(
                "SELECT account_id, lifecycle, version
                 FROM product_genesis_session WHERE id = ?",
            )
            .bind(genesis_session_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
            let genesis_account: String = genesis.try_get("account_id")?;
            let genesis_lifecycle: String = genesis.try_get("lifecycle")?;
            if genesis_account != input.account_id
                || !matches!(
                    genesis_lifecycle.as_str(),
                    "discovering" | "ready_for_project"
                )
            {
                return Err(DbError::VersionConflict);
            }
        }
        sqlx::query(
            "INSERT INTO project_charter (
                id, account_id, genesis_session_id, project_mode, maturity,
                lifecycle, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'draft', 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.account_id)
        .bind(input.genesis_session_id.as_deref())
        .bind(&input.project_mode)
        .bind(&input.maturity)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
            let updated = sqlx::query(
                "UPDATE product_genesis_session
                 SET charter_id = ?, charter_version = 1, version = version + 1, updated_at = ?
                 WHERE id = ? AND account_id = ?
                   AND lifecycle IN ('discovering', 'ready_for_project')",
            )
            .bind(&input.id)
            .bind(&input.updated_at)
            .bind(genesis_session_id)
            .bind(&input.account_id)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?;
            if updated.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
        let row = sqlx::query("SELECT * FROM project_charter WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_charter(row)
    }

    async fn get_project_charter_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCharterRevisionRecord>> {
        select_one(
            "SELECT * FROM project_charter_revision WHERE id = ?",
            self.pool(),
            id,
            map_charter_revision,
        )
        .await
    }

    async fn list_project_charter_revisions(
        &self,
        charter_id: &str,
    ) -> Result<Vec<ProjectCharterRevisionRecord>> {
        sqlx::query(
            "SELECT * FROM project_charter_revision
             WHERE charter_id = ? ORDER BY revision ASC, id ASC",
        )
        .bind(charter_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_charter_revision)
        .collect()
    }

    async fn create_project_charter_revision(
        &self,
        input: CreateProjectCharterRevision,
    ) -> Result<ProjectCharterRevisionRecord> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        let charter = sqlx::query(
            "SELECT account_id, version, current_draft_revision_id
             FROM project_charter WHERE id = ?",
        )
        .bind(&input.charter_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let charter_account_id: String = charter.try_get("account_id")?;
        let charter_version: i64 = charter.try_get("version")?;
        let current_draft: Option<String> = charter.try_get("current_draft_revision_id")?;
        if charter_version != input.expected_charter_version {
            return Err(DbError::VersionConflict);
        }
        if input.base_revision > 0 {
            let Some(current_draft) = current_draft else {
                return Err(DbError::VersionConflict);
            };
            let Some(base_revision_id) = input.base_revision_id.as_deref() else {
                return Err(DbError::VersionConflict);
            };
            if current_draft != base_revision_id {
                return Err(DbError::VersionConflict);
            }
            let base_ok: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_charter_revision
                 WHERE id = ? AND charter_id = ? AND revision = ? LIMIT 1",
            )
            .bind(base_revision_id)
            .bind(&input.charter_id)
            .bind(input.base_revision)
            .fetch_optional(&mut *transaction)
            .await?;
            if base_ok.is_none() {
                return Err(DbError::VersionConflict);
            }
        } else if input.base_revision_id.is_some() || current_draft.is_some() {
            return Err(DbError::VersionConflict);
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_charter_revision WHERE charter_id = ?",
        )
        .bind(&input.charter_id)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO project_charter_revision (
                id, charter_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_message_id,
                source_turn_job_id, source_refs_json, content_digest,
                rendered_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.charter_id)
        .bind(revision)
        .bind(input.base_revision)
        .bind(input.base_revision_id.as_deref())
        .bind(&input.lifecycle)
        .bind(&input.schema_version)
        .bind(&input.render_version)
        .bind(&input.content_json)
        .bind(&input.rendered_view)
        .bind(&input.change_summary)
        .bind(&input.author_type)
        .bind(input.author_id.as_deref())
        .bind(input.source_message_id.as_deref())
        .bind(input.source_turn_job_id.as_deref())
        .bind(&input.source_refs_json)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let charter_update = sqlx::query(
            "UPDATE project_charter
             SET current_draft_revision_id = ?, project_mode = ?, maturity = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.id)
        .bind(&input.project_mode)
        .bind(&input.maturity)
        .bind(&input.created_at)
        .bind(&input.charter_id)
        .bind(input.expected_charter_version)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        if charter_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let genesis: Option<(String, String)> = sqlx::query_as(
            "SELECT id, account_id FROM product_genesis_session
             WHERE id = (SELECT genesis_session_id FROM project_charter WHERE id = ?)
               AND lifecycle IN ('discovering', 'ready_for_project')",
        )
        .bind(&input.charter_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some((genesis_id, genesis_account)) = genesis {
            let genesis_update = sqlx::query(
                "UPDATE product_genesis_session
                 SET charter_version = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND account_id = ?",
            )
            .bind(input.expected_charter_version + 1)
            .bind(&input.created_at)
            .bind(genesis_id)
            .bind(genesis_account)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;
            if genesis_update.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project_charter.revision_created".to_owned(),
            entity_type: "project_charter_revision".to_owned(),
            entity_id: input.id.clone(),
            actor_type: input.author_type.clone(),
            actor_id: input.author_id.clone(),
            scope_type: "account".to_owned(),
            scope_id: charter_account_id,
            correlation_id: input.id.clone(),
            causation_id: input.source_message_id.clone(),
            causation_depth: 0,
            dedupe_key: Some(format!("project-charter-revision-created:{}", input.id)),
            payload_json: serde_json::json!({
                "charter_id": input.charter_id.clone(),
                "revision_id": input.id.clone(),
                "revision": revision,
                "content_digest": input.content_digest.clone(),
                "rendered_digest": input.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        transaction.commit().await?;
        self.get_project_charter_revision(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_project_charter_revision_atomically(
        &self,
        input: CreateProjectCharterRevisionAtomically,
    ) -> Result<ProjectCharterRevisionRecord> {
        if input.charter.id != input.revision.charter_id
            || input.charter.account_id != input.account_id
            || input.charter.genesis_session_id != input.genesis_session_id
            || input.revision.expected_charter_version != 1
            || input.revision.base_revision != 0
            || input.revision.base_revision_id.is_some()
            || (input.project_id.is_none() && input.genesis_session_id.is_none())
            || (input.project_id.is_some() && input.genesis_session_id.is_some())
        {
            return Err(DbError::VersionConflict);
        }

        let mut transaction = crate::begin_immediate(self.pool()).await?;
        if let Some(project_id) = input.project_id.as_deref() {
            let project = sqlx::query("SELECT id, owner_id FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(DbError::NotFound)?;
            let owner_id: Option<String> = project.try_get("owner_id")?;
            let privileged_member: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_member
                 WHERE project_id = ? AND user_id = ? AND role IN ('owner', 'admin')
                 LIMIT 1",
            )
            .bind(project_id)
            .bind(&input.account_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if owner_id.as_deref() != Some(input.account_id.as_str()) && privileged_member.is_none()
            {
                return Err(DbError::VersionConflict);
            }
        } else if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
            let genesis = sqlx::query(
                "SELECT account_id, lifecycle, charter_id
                 FROM product_genesis_session WHERE id = ?",
            )
            .bind(genesis_session_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let genesis_account_id: String = genesis.try_get("account_id")?;
            let genesis_lifecycle: String = genesis.try_get("lifecycle")?;
            let genesis_charter_id: Option<String> = genesis.try_get("charter_id")?;
            if genesis_account_id != input.account_id
                || !matches!(
                    genesis_lifecycle.as_str(),
                    "discovering" | "ready_for_project"
                )
                || genesis_charter_id.is_some_and(|id| id != input.charter.id)
            {
                return Err(DbError::VersionConflict);
            }
        }

        let existing = sqlx::query(
            "SELECT account_id, project_id, genesis_session_id, project_mode, maturity
             FROM project_charter WHERE id = ?",
        )
        .bind(&input.charter.id)
        .fetch_optional(&mut *transaction)
        .await?;

        if let Some(existing) = existing {
            let account_id: String = existing.try_get("account_id")?;
            let project_id: Option<String> = existing.try_get("project_id")?;
            let genesis_session_id: Option<String> = existing.try_get("genesis_session_id")?;
            let project_mode: String = existing.try_get("project_mode")?;
            let maturity: String = existing.try_get("maturity")?;
            if account_id != input.account_id
                || project_id
                    .as_deref()
                    .is_some_and(|existing| input.project_id.as_deref() != Some(existing))
                || genesis_session_id
                    .as_deref()
                    .is_some_and(|existing| input.genesis_session_id.as_deref() != Some(existing))
                || project_mode != input.revision.project_mode
                || maturity != input.revision.maturity
            {
                return Err(DbError::VersionConflict);
            }
            if input.project_id.is_some() && project_id.is_none() {
                let claimed = sqlx::query(
                    "UPDATE project_charter SET project_id = ?, updated_at = ?
                     WHERE id = ? AND account_id = ? AND project_id IS NULL
                       AND genesis_session_id IS NULL",
                )
                .bind(input.project_id.as_deref())
                .bind(&input.revision.created_at)
                .bind(&input.charter.id)
                .bind(&input.account_id)
                .execute(&mut *transaction)
                .await
                .map_err(orchestration_write_error)?;
                if claimed.rows_affected() != 1 {
                    return Err(DbError::VersionConflict);
                }
            } else if input.genesis_session_id.is_some() && genesis_session_id.is_none() {
                let claimed = sqlx::query(
                    "UPDATE project_charter SET genesis_session_id = ?, updated_at = ?
                     WHERE id = ? AND account_id = ? AND project_id IS NULL
                       AND genesis_session_id IS NULL",
                )
                .bind(input.genesis_session_id.as_deref())
                .bind(&input.revision.created_at)
                .bind(&input.charter.id)
                .bind(&input.account_id)
                .execute(&mut *transaction)
                .await
                .map_err(orchestration_write_error)?;
                if claimed.rows_affected() != 1 {
                    return Err(DbError::VersionConflict);
                }
            }
        } else {
            sqlx::query(
                "INSERT INTO project_charter (
                    id, account_id, genesis_session_id, project_id,
                    project_mode, maturity, lifecycle, version,
                    created_at, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, 'draft', 1, ?, ?)",
            )
            .bind(&input.charter.id)
            .bind(&input.account_id)
            .bind(input.genesis_session_id.as_deref())
            .bind(&input.project_id)
            .bind(&input.revision.project_mode)
            .bind(&input.revision.maturity)
            .bind(&input.charter.created_at)
            .bind(&input.charter.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?;
            if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
                let linked = sqlx::query(
                    "UPDATE product_genesis_session
                     SET charter_id = ?, charter_version = 1,
                         version = version + 1, updated_at = ?
                     WHERE id = ? AND account_id = ? AND charter_id IS NULL
                       AND lifecycle IN ('discovering', 'ready_for_project')",
                )
                .bind(&input.charter.id)
                .bind(&input.revision.created_at)
                .bind(genesis_session_id)
                .bind(&input.account_id)
                .execute(&mut *transaction)
                .await
                .map_err(orchestration_write_error)?;
                if linked.rows_affected() != 1 {
                    return Err(DbError::VersionConflict);
                }
            }
        }

        if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
            let linked_charter_id: Option<String> =
                sqlx::query_scalar("SELECT charter_id FROM product_genesis_session WHERE id = ?")
                    .bind(genesis_session_id)
                    .fetch_one(&mut *transaction)
                    .await?;
            match linked_charter_id {
                Some(existing) if existing != input.charter.id => {
                    return Err(DbError::VersionConflict);
                }
                Some(_) => {}
                None => {
                    let linked = sqlx::query(
                        "UPDATE product_genesis_session
                         SET charter_id = ?, charter_version = 1,
                             version = version + 1, updated_at = ?
                         WHERE id = ? AND account_id = ? AND charter_id IS NULL
                           AND lifecycle IN ('discovering', 'ready_for_project')",
                    )
                    .bind(&input.charter.id)
                    .bind(&input.revision.created_at)
                    .bind(genesis_session_id)
                    .bind(&input.account_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(orchestration_write_error)?;
                    if linked.rows_affected() != 1 {
                        return Err(DbError::VersionConflict);
                    }
                }
            }
        }

        let ownership_column = if input.project_id.is_some() {
            "project_id"
        } else {
            "genesis_session_id"
        };
        let charter = sqlx::query(&format!(
            "SELECT account_id, version, current_draft_revision_id
             FROM project_charter WHERE id = ? AND {ownership_column} = ?"
        ))
        .bind(&input.revision.charter_id)
        .bind(
            input
                .project_id
                .as_deref()
                .or(input.genesis_session_id.as_deref()),
        )
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let charter_account_id: String = charter.try_get("account_id")?;
        let charter_version: i64 = charter.try_get("version")?;
        let current_draft: Option<String> = charter.try_get("current_draft_revision_id")?;
        if charter_account_id != input.account_id
            || charter_version != input.revision.expected_charter_version
            || current_draft.is_some()
        {
            return Err(DbError::VersionConflict);
        }

        let revision_number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_charter_revision WHERE charter_id = ?",
        )
        .bind(&input.revision.charter_id)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO project_charter_revision (
                id, charter_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_message_id,
                source_turn_job_id, source_refs_json, content_digest,
                rendered_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.charter_id)
        .bind(revision_number)
        .bind(input.revision.base_revision)
        .bind(input.revision.base_revision_id.as_deref())
        .bind(&input.revision.lifecycle)
        .bind(&input.revision.schema_version)
        .bind(&input.revision.render_version)
        .bind(&input.revision.content_json)
        .bind(&input.revision.rendered_view)
        .bind(&input.revision.change_summary)
        .bind(&input.revision.author_type)
        .bind(input.revision.author_id.as_deref())
        .bind(input.revision.source_message_id.as_deref())
        .bind(input.revision.source_turn_job_id.as_deref())
        .bind(&input.revision.source_refs_json)
        .bind(&input.revision.content_digest)
        .bind(&input.revision.rendered_digest)
        .bind(&input.revision.created_at)
        .execute(&mut *transaction)
        .await
        .map_err(orchestration_write_error)?;
        let charter_update = if let Some(project_id) = input.project_id.as_deref() {
            sqlx::query(
                "UPDATE project_charter
                 SET current_draft_revision_id = ?, project_mode = ?, maturity = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?
                   AND current_draft_revision_id IS NULL",
            )
            .bind(&input.revision.id)
            .bind(&input.revision.project_mode)
            .bind(&input.revision.maturity)
            .bind(&input.revision.created_at)
            .bind(&input.revision.charter_id)
            .bind(project_id)
            .bind(input.revision.expected_charter_version)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?
        } else {
            let genesis_session_id = input
                .genesis_session_id
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            sqlx::query(
                "UPDATE project_charter
                 SET current_draft_revision_id = ?, project_mode = ?, maturity = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND genesis_session_id = ? AND version = ?
                   AND current_draft_revision_id IS NULL",
            )
            .bind(&input.revision.id)
            .bind(&input.revision.project_mode)
            .bind(&input.revision.maturity)
            .bind(&input.revision.created_at)
            .bind(&input.revision.charter_id)
            .bind(genesis_session_id)
            .bind(input.revision.expected_charter_version)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?
        };
        if charter_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project_charter.revision_created".to_owned(),
            entity_type: "project_charter_revision".to_owned(),
            entity_id: input.revision.id.clone(),
            actor_type: input.revision.author_type.clone(),
            actor_id: input.revision.author_id.clone(),
            scope_type: "account".to_owned(),
            scope_id: charter_account_id,
            correlation_id: input.revision.id.clone(),
            causation_id: input.revision.source_message_id.clone(),
            causation_depth: 0,
            dedupe_key: Some(format!(
                "project-charter-revision-created:{}",
                input.revision.id
            )),
            payload_json: serde_json::json!({
                "charter_id": input.revision.charter_id.clone(),
                "revision_id": input.revision.id.clone(),
                "revision": revision_number,
                "content_digest": input.revision.content_digest.clone(),
                "rendered_digest": input.revision.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.revision.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        let row = sqlx::query("SELECT * FROM project_charter_revision WHERE id = ?")
            .bind(&input.revision.id)
            .fetch_one(&mut *transaction)
            .await?;
        let record = map_charter_revision(row)?;
        transaction.commit().await?;
        Ok(record)
    }

    async fn get_project_charter_approval(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCharterApprovalRecord>> {
        select_one(
            "SELECT * FROM project_charter_approval WHERE id = ?",
            self.pool(),
            id,
            map_charter_approval,
        )
        .await
    }

    async fn approve_project_charter(
        &self,
        input: ApproveProjectCharter,
    ) -> Result<ProjectCharterApprovalRecord> {
        if !valid_authorization_timestamp(&input.authorization_occurred_at) {
            return Err(DbError::VersionConflict);
        }
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        let charter_scope =
            sqlx::query("SELECT account_id, project_id FROM project_charter WHERE id = ?")
                .bind(&input.charter_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(DbError::NotFound)?;
        let account_id: String = charter_scope.try_get("account_id")?;
        let project_id: Option<String> = charter_scope.try_get("project_id")?;
        let scope_id = project_id.unwrap_or_else(|| format!("account:{account_id}"));
        let storage_idempotency_key = orchestration_scoped_idempotency_key(
            "charter-approval",
            &scope_id,
            &input.approving_principal_id,
            &input.idempotency_key,
        );

        if let Some(existing) =
            sqlx::query("SELECT * FROM project_charter_approval WHERE idempotency_key = ?")
                .bind(&storage_idempotency_key)
                .fetch_optional(&mut *transaction)
                .await?
                .map(map_charter_approval)
                .transpose()?
        {
            let same = existing.charter_id == input.charter_id
                && existing.revision_id == input.revision_id
                && existing.content_digest == input.content_digest
                && existing.rendered_digest == input.rendered_digest
                && existing.expected_charter_version == input.expected_charter_version
                && existing.approval_type == input.approval_type
                && existing.approved_name == input.approved_name
                && existing.approved_slug == input.approved_slug
                && existing.approved_project_mode == input.approved_project_mode
                && existing.selected_identity_id == input.selected_identity_id
                && existing.selected_profile_id == input.selected_profile_id
                && existing.selected_operating_skill_revision_id
                    == input.selected_operating_skill_revision_id
                && existing.selected_policy_revision == input.selected_policy_revision
                && existing.selected_policy_digest == input.selected_policy_digest
                && existing.approving_principal_type == input.approving_principal_type
                && existing.approving_principal_id == input.approving_principal_id
                && existing.authorization_basis == input.authorization_basis
                && existing.authorization_action == input.authorization_action
                && existing.explicit_event == input.explicit_event
                && existing.authorization_occurred_at == input.authorization_occurred_at
                && existing.source_action == input.source_action;
            if !same {
                return Err(DbError::VersionConflict);
            }
            transaction.commit().await?;
            return Ok(existing);
        }

        let target = sqlx::query(
            "SELECT c.version AS charter_version, c.account_id, c.project_mode,
                    c.current_approved_revision_id AS previous_approved_revision_id,
                    r.charter_id, r.lifecycle, r.content_digest, r.rendered_digest
             FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND r.charter_id = ?",
        )
        .bind(&input.revision_id)
        .bind(&input.charter_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let version: i64 = target.try_get("charter_version")?;
        let lifecycle: String = target.try_get("lifecycle")?;
        let content_digest: String = target.try_get("content_digest")?;
        let rendered_digest: String = target.try_get("rendered_digest")?;
        let project_mode: String = target.try_get("project_mode")?;
        if version != input.expected_charter_version
            || content_digest != input.content_digest
            || rendered_digest != input.rendered_digest
            || project_mode != input.approved_project_mode
            || !matches!(lifecycle.as_str(), "draft" | "proposed" | "approved")
        {
            return Err(DbError::VersionConflict);
        }

        if input.selected_identity_id.is_some() != input.selected_profile_id.is_some() {
            return Err(DbError::Check(
                "Project Agent identity and profile must be selected together".to_owned(),
            ));
        }
        if input.approval_type == "project_creation"
            && (input.selected_identity_id.is_none()
                || input.selected_profile_id.is_none()
                || input.selected_operating_skill_revision_id.is_none()
                || input.selected_policy_revision.is_none()
                || input.selected_policy_digest.is_none())
        {
            return Err(DbError::VersionConflict);
        }
        if let (Some(identity_id), Some(profile_id)) =
            (&input.selected_identity_id, &input.selected_profile_id)
        {
            let skill_revision_id = input
                .selected_operating_skill_revision_id
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            let selected = sqlx::query(
                "SELECT p.tool_policy_json, i.paused, i.archived_at,
                        i.selected_profile_id, sr.id AS skill_revision_id,
                        sr.skill_key, s.current_revision_id, s.lifecycle
                 FROM agent_profile p
                 JOIN agent_identity i ON i.id = p.identity_id
                 JOIN project_charter c ON c.account_id = i.owner_id
                 JOIN operating_skill_revision sr ON sr.id = ?
                 JOIN operating_skill s ON s.id = sr.operating_skill_id
                 WHERE p.id = ? AND p.identity_id = ? AND c.id = ?
                   AND i.selected_profile_id = p.id
                 LIMIT 1",
            )
            .bind(skill_revision_id)
            .bind(profile_id)
            .bind(identity_id)
            .bind(&input.charter_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(selected) = selected else {
                return Err(DbError::Check(
                    "selected Project Agent profile is not owned by the Charter account".to_owned(),
                ));
            };
            let selected_paused: i64 = selected.try_get("paused")?;
            let selected_archived: Option<String> = selected.try_get("archived_at")?;
            let selected_profile_id: Option<String> = selected.try_get("selected_profile_id")?;
            let selected_skill_revision_id: String = selected.try_get("skill_revision_id")?;
            let selected_skill_key: String = selected.try_get("skill_key")?;
            let selected_skill_current_revision_id: Option<String> =
                selected.try_get("current_revision_id")?;
            let selected_skill_lifecycle: String = selected.try_get("lifecycle")?;
            let selected_tool_policy_json: String = selected.try_get("tool_policy_json")?;
            let selected_policy_digest = profile_policy_digest(&selected_tool_policy_json);
            if selected_paused != 0
                || selected_archived.is_some()
                || selected_profile_id.as_deref() != Some(profile_id.as_str())
                || selected_skill_revision_id != skill_revision_id
                || selected_skill_current_revision_id.as_deref() != Some(skill_revision_id)
                || selected_skill_lifecycle != "active"
                || selected_skill_key != PROJECT_OPERATING_SKILL_KEY
                || input.selected_policy_digest.as_deref() != Some(selected_policy_digest.as_str())
            {
                return Err(DbError::VersionConflict);
            }
        }

        let previous_active_approval: Option<String> = sqlx::query_scalar(
            "SELECT id FROM project_charter_approval
             WHERE charter_id = ? AND lifecycle = 'active' AND id != ? LIMIT 1",
        )
        .bind(&input.charter_id)
        .bind(&input.id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(previous_approval_id) = previous_active_approval {
            let revoked = sqlx::query(
                "UPDATE project_charter_approval SET lifecycle = 'revoked',
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND lifecycle = 'active'",
            )
            .bind(&input.updated_at)
            .bind(&previous_approval_id)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;
            if revoked.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
            sqlx::query(
                "INSERT INTO project_charter_approval_event (
                    id, approval_id, lifecycle, principal_type, principal_id,
                    authorization_basis, action, explicit_event, reason,
                    idempotency_key, occurred_at, created_at
                 ) VALUES (?, ?, 'revoked', ?, ?, ?, ?, ?, 'Superseded by newer approval', ?, ?, ?)",
            )
            .bind(new_uuid_v4())
            .bind(&previous_approval_id)
            .bind(&input.approving_principal_type)
            .bind(&input.approving_principal_id)
            .bind(&input.authorization_basis)
            .bind(&input.authorization_action)
            .bind(&input.explicit_event)
            .bind(format!(
                "{}:revoke:{}",
                storage_idempotency_key, previous_approval_id
            ))
            .bind(&input.authorization_occurred_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;
        }
        let previous_approved_revision: Option<String> =
            target.try_get("previous_approved_revision_id")?;
        if let Some(previous_revision_id) = previous_approved_revision {
            if previous_revision_id != input.revision_id {
                let superseded = sqlx::query(
                    "UPDATE project_charter_revision SET lifecycle = 'superseded'
                     WHERE id = ? AND charter_id = ? AND lifecycle = 'approved'",
                )
                .bind(previous_revision_id)
                .bind(&input.charter_id)
                .execute(&mut *transaction)
                .await
                .map_err(check_error)?;
                if superseded.rows_affected() != 1 {
                    return Err(DbError::VersionConflict);
                }
            }
        }

        let approved_revision = sqlx::query(
            "UPDATE project_charter_revision
             SET lifecycle = CASE WHEN id = ? THEN 'approved' ELSE lifecycle END
             WHERE id = ?",
        )
        .bind(&input.revision_id)
        .bind(&input.revision_id)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        if approved_revision.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let charter_update = sqlx::query(
            "UPDATE project_charter
             SET current_approved_revision_id = ?, lifecycle = 'ready_for_approval',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.revision_id)
        .bind(&input.updated_at)
        .bind(&input.charter_id)
        .bind(input.expected_charter_version)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if error.to_string().contains("UNIQUE") {
                DbError::VersionConflict
            } else {
                check_error(error)
            }
        })?;
        if charter_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        sqlx::query(
            "INSERT INTO project_charter_approval (
                id, approval_type, charter_id, revision_id, content_digest,
                rendered_digest, expected_charter_version, approved_name,
                approved_slug, selected_identity_id, selected_profile_id,
                selected_operating_skill_revision_id, selected_policy_revision,
                selected_policy_digest, approving_principal_type,
                approving_principal_id, authorization_basis, authorization_action,
                explicit_event, authorization_occurred_at, source_action,
                lifecycle, idempotency_key, version,
                created_at, updated_at, approved_project_mode, approval_event_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'active', ?, 1, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.approval_type)
        .bind(&input.charter_id)
        .bind(&input.revision_id)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(input.expected_charter_version)
        .bind(input.approved_name.as_deref())
        .bind(input.approved_slug.as_deref())
        .bind(input.selected_identity_id.as_deref())
        .bind(input.selected_profile_id.as_deref())
        .bind(input.selected_operating_skill_revision_id.as_deref())
        .bind(input.selected_policy_revision.as_deref())
        .bind(input.selected_policy_digest.as_deref())
        .bind(&input.approving_principal_type)
        .bind(&input.approving_principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.explicit_event)
        .bind(&input.authorization_occurred_at)
        .bind(&input.source_action)
        .bind(&storage_idempotency_key)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .bind(&input.approved_project_mode)
        .bind(Option::<String>::None)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if error.to_string().contains("UNIQUE") {
                DbError::VersionConflict
            } else {
                check_error(error)
            }
        })?;
        sqlx::query(
            "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, idempotency_key,
                occurred_at, created_at
             ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.event_id)
        .bind(&input.id)
        .bind(&input.approving_principal_type)
        .bind(&input.approving_principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.explicit_event)
        .bind(format!("{storage_idempotency_key}:active"))
        .bind(&input.authorization_occurred_at)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let receipt_event = sqlx::query(
            "UPDATE project_charter_approval
             SET approval_event_id = ?
             WHERE id = ? AND approval_event_id IS NULL",
        )
        .bind(&input.event_id)
        .bind(&input.id)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        if receipt_event.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let genesis_update = sqlx::query(
            "UPDATE product_genesis_session
             SET charter_revision_id = ?, charter_approval_id = ?, charter_version = ?,
                 lifecycle = 'ready_for_project', version = version + 1, updated_at = ?
             WHERE charter_id = ? AND account_id = ?
               AND lifecycle IN ('discovering', 'ready_for_project')",
        )
        .bind(&input.revision_id)
        .bind(&input.id)
        .bind(input.expected_charter_version + 1)
        .bind(&input.updated_at)
        .bind(&input.charter_id)
        .bind(&target.try_get::<String, _>("account_id")?)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        if genesis_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project_charter.approved".to_owned(),
            entity_type: "project_charter_approval".to_owned(),
            entity_id: input.id.clone(),
            actor_type: input.approving_principal_type.clone(),
            actor_id: Some(input.approving_principal_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: target.try_get::<String, _>("account_id")?,
            correlation_id: input.id.clone(),
            causation_id: Some(input.event_id.clone()),
            causation_depth: 0,
            dedupe_key: Some(format!("project-charter-approved:{}", input.id)),
            payload_json: serde_json::json!({
                "approval_id": input.id.clone(),
                "charter_id": input.charter_id.clone(),
                "revision_id": input.revision_id.clone(),
                "approval_event_id": input.event_id.clone(),
                "content_digest": input.content_digest.clone(),
                "rendered_digest": input.rendered_digest.clone(),
                "approved_project_mode": input.approved_project_mode.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        transaction.commit().await?;
        self.get_project_charter_approval(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_project_document(
        &self,
        input: CreateProjectDocument,
    ) -> Result<ProjectDocumentRecord> {
        sqlx::query(
            "INSERT INTO project_document (
                id, project_id, kind, title, lifecycle, approval_policy,
                current_draft_revision_id, current_approved_revision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'draft', ?, NULL, NULL, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.kind)
        .bind(&input.title)
        .bind(&input.approval_policy)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(self.pool())
        .await
        .map_err(check_error)?;
        self.get_project_document(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_project_document(&self, id: &str) -> Result<Option<ProjectDocumentRecord>> {
        select_one(
            "SELECT * FROM project_document WHERE id = ?",
            self.pool(),
            id,
            map_document,
        )
        .await
    }

    async fn create_project_document_revision(
        &self,
        input: CreateProjectDocumentRevision,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let document = sqlx::query(
            "SELECT project_id, version, current_draft_revision_id
             FROM project_document WHERE id = ?",
        )
        .bind(&input.document_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let document_version: i64 = document.try_get("version")?;
        if document_version != input.expected_document_version {
            return Err(DbError::VersionConflict);
        }
        let current_draft: Option<String> = document.try_get("current_draft_revision_id")?;
        if input.base_revision > 0 {
            let Some(current_draft) = current_draft else {
                return Err(DbError::VersionConflict);
            };
            let Some(base_revision_id) = input.base_revision_id.as_deref() else {
                return Err(DbError::VersionConflict);
            };
            if current_draft != base_revision_id {
                return Err(DbError::VersionConflict);
            }
            let base_matches: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_document_revision
                 WHERE id = ? AND document_id = ? AND revision = ? LIMIT 1",
            )
            .bind(base_revision_id)
            .bind(&input.document_id)
            .bind(input.base_revision)
            .fetch_optional(&mut *tx)
            .await?;
            if base_matches.is_none() {
                return Err(DbError::VersionConflict);
            }
        } else if input.base_revision_id.is_some() || current_draft.is_some() {
            return Err(DbError::VersionConflict);
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_document_revision WHERE document_id = ?",
        )
        .bind(&input.document_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO project_document_revision (
                id, document_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_refs_json,
                content_digest, rendered_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.document_id)
        .bind(revision)
        .bind(input.base_revision)
        .bind(input.base_revision_id.as_deref())
        .bind(&input.lifecycle)
        .bind(&input.schema_version)
        .bind(&input.render_version)
        .bind(&input.content_json)
        .bind(&input.rendered_view)
        .bind(&input.change_summary)
        .bind(&input.author_type)
        .bind(input.author_id.as_deref())
        .bind(&input.source_refs_json)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let updated = sqlx::query(
            "UPDATE project_document
             SET current_draft_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.id)
        .bind(&input.created_at)
        .bind(&input.document_id)
        .bind(input.expected_document_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let project_id: String = document.try_get("project_id")?;
        DomainEventRepo::append_event_in_tx(
            self,
            &mut tx,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "project.document.revision_created".to_owned(),
                entity_type: "project_document_revision".to_owned(),
                entity_id: input.id.clone(),
                actor_type: input.author_type.clone(),
                actor_id: input.author_id.clone(),
                scope_type: "project".to_owned(),
                scope_id: project_id,
                correlation_id: input.id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("project-document-revision-created:{}", input.id)),
                payload_json: serde_json::json!({
                    "document_id": input.document_id.clone(),
                    "revision_id": input.id.clone(),
                    "revision": revision,
                    "lifecycle": input.lifecycle.clone(),
                    "content_digest": input.content_digest.clone(),
                    "rendered_digest": input.rendered_digest.clone(),
                })
                .to_string(),
                created_at: input.created_at.clone(),
            },
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_document_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_document_revision(row)
    }

    async fn create_project_document_atomically(
        &self,
        input: CreateProjectDocumentAtomically,
    ) -> Result<ProjectDocumentRevisionRecord> {
        if input.document.id != input.revision.document_id
            || input.revision.expected_document_version != 1
            || input.revision.base_revision != 0
            || input.revision.base_revision_id.is_some()
        {
            return Err(DbError::VersionConflict);
        }

        let mut tx = crate::begin_immediate(self.pool()).await?;
        sqlx::query(
            "INSERT INTO project_document (
                id, project_id, kind, title, lifecycle, approval_policy,
                current_draft_revision_id, current_approved_revision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'draft', ?, NULL, NULL, 1, ?, ?)",
        )
        .bind(&input.document.id)
        .bind(&input.document.project_id)
        .bind(&input.document.kind)
        .bind(&input.document.title)
        .bind(&input.document.approval_policy)
        .bind(&input.document.created_at)
        .bind(&input.document.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        sqlx::query(
            "INSERT INTO project_document_revision (
                id, document_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_refs_json,
                content_digest, rendered_digest, created_at
             ) VALUES (?, ?, 1, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.document_id)
        .bind(&input.revision.lifecycle)
        .bind(&input.revision.schema_version)
        .bind(&input.revision.render_version)
        .bind(&input.revision.content_json)
        .bind(&input.revision.rendered_view)
        .bind(&input.revision.change_summary)
        .bind(&input.revision.author_type)
        .bind(input.revision.author_id.as_deref())
        .bind(&input.revision.source_refs_json)
        .bind(&input.revision.content_digest)
        .bind(&input.revision.rendered_digest)
        .bind(&input.revision.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let updated = sqlx::query(
            "UPDATE project_document
             SET current_draft_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = 1",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.created_at)
        .bind(&input.document.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        DomainEventRepo::append_event_in_tx(
            self,
            &mut tx,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "project.document.revision_created".to_owned(),
                entity_type: "project_document_revision".to_owned(),
                entity_id: input.revision.id.clone(),
                actor_type: input.revision.author_type.clone(),
                actor_id: input.revision.author_id.clone(),
                scope_type: "project".to_owned(),
                scope_id: input.document.project_id.clone(),
                correlation_id: input.revision.id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!(
                    "project-document-revision-created:{}",
                    input.revision.id
                )),
                payload_json: serde_json::json!({
                    "document_id": input.revision.document_id.clone(),
                    "revision_id": input.revision.id.clone(),
                    "revision": 1,
                    "lifecycle": input.revision.lifecycle.clone(),
                    "content_digest": input.revision.content_digest.clone(),
                    "rendered_digest": input.revision.rendered_digest.clone(),
                })
                .to_string(),
                created_at: input.revision.created_at.clone(),
            },
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_document_revision WHERE id = ?")
            .bind(&input.revision.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_document_revision(row)
    }

    async fn get_project_document_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectDocumentRevisionRecord>> {
        select_one(
            "SELECT * FROM project_document_revision WHERE id = ?",
            self.pool(),
            id,
            map_document_revision,
        )
        .await
    }

    async fn list_project_document_revisions(
        &self,
        document_id: &str,
    ) -> Result<Vec<ProjectDocumentRevisionRecord>> {
        sqlx::query(
            "SELECT * FROM project_document_revision
             WHERE document_id = ? ORDER BY revision ASC, id ASC",
        )
        .bind(document_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_document_revision)
        .collect()
    }

    async fn approve_project_document(
        &self,
        input: ApproveProjectDocument,
    ) -> Result<ProjectDocumentApprovalRecord> {
        if input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) =
            sqlx::query("SELECT * FROM project_document_approval WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut *tx)
                .await?
                .map(map_document_approval)
                .transpose()?
        {
            if existing.document_id != input.document_id
                || existing.revision_id != input.revision_id
                || existing.content_digest != input.content_digest
                || existing.rendered_digest != input.rendered_digest
                || existing.principal_type != input.principal_type
                || existing.principal_id != input.principal_id
                || existing.authorization_basis != input.authorization_basis
                || existing.authorization_action != input.authorization_action
                || existing.authorization_occurred_at != input.authorization_occurred_at
                || existing.explicit_event != input.explicit_event
            {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let document = sqlx::query("SELECT project_id, version FROM project_document WHERE id = ?")
            .bind(&input.document_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let document_version: i64 = document.try_get("version")?;
        if document_version != input.expected_document_version {
            return Err(DbError::VersionConflict);
        }
        let target = sqlx::query(
            "SELECT lifecycle FROM project_document_revision
             WHERE id = ? AND document_id = ? AND content_digest = ?
               AND rendered_digest = ?",
        )
        .bind(&input.revision_id)
        .bind(&input.document_id)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let target_lifecycle: String = target.try_get("lifecycle")?;
        if matches!(
            target_lifecycle.as_str(),
            "rejected" | "withdrawn" | "superseded"
        ) {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "UPDATE project_document_revision SET lifecycle = 'superseded'
             WHERE document_id = ? AND lifecycle = 'approved' AND id != ?",
        )
        .bind(&input.document_id)
        .bind(&input.revision_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let approved = sqlx::query(
            "UPDATE project_document_revision SET lifecycle = 'approved'
             WHERE id = ? AND document_id = ? AND lifecycle != 'approved'",
        )
        .bind(&input.revision_id)
        .bind(&input.document_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if approved.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let updated = sqlx::query(
            "UPDATE project_document
             SET current_approved_revision_id = ?, lifecycle = 'approved',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.revision_id)
        .bind(&input.updated_at)
        .bind(&input.document_id)
        .bind(input.expected_document_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_document_approval (
                id, document_id, revision_id, principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, content_digest, rendered_digest,
                lifecycle, idempotency_key, version,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.document_id)
        .bind(&input.revision_id)
        .bind(&input.principal_type)
        .bind(&input.principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.explicit_event)
        .bind(&input.authorization_occurred_at)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(&input.idempotency_key)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let project_id: String = document.try_get("project_id")?;
        DomainEventRepo::append_event_in_tx(
            self,
            &mut tx,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "project.document.approved".to_owned(),
                entity_type: "project_document_approval".to_owned(),
                entity_id: input.id.clone(),
                actor_type: input.principal_type.clone(),
                actor_id: Some(input.principal_id.clone()),
                scope_type: "project".to_owned(),
                scope_id: project_id,
                correlation_id: input.id.clone(),
                causation_id: Some(input.explicit_event.clone()),
                causation_depth: 0,
                dedupe_key: Some(format!("project-document-approved:{}", input.id)),
                payload_json: serde_json::json!({
                    "document_id": input.document_id.clone(),
                    "revision_id": input.revision_id.clone(),
                    "approval_id": input.id.clone(),
                    "content_digest": input.content_digest.clone(),
                    "rendered_digest": input.rendered_digest.clone(),
                })
                .to_string(),
                created_at: input.created_at.clone(),
            },
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_document_approval WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_document_approval(row)
    }

    async fn create_project_decision_candidate(
        &self,
        input: CreateProjectDecisionCandidate,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_decision_candidate (
                id, project_id, lifecycle, question, context_json, options_json,
                selected_outcome, rationale, principal_type, principal_id,
                source_refs_json, expected_project_version, effective_decision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.lifecycle)
        .bind(&input.question)
        .bind(&input.context_json)
        .bind(&input.options_json)
        .bind(input.selected_outcome.as_deref())
        .bind(input.rationale.as_deref())
        .bind(input.principal_type.as_deref())
        .bind(input.principal_id.as_deref())
        .bind(&input.source_refs_json)
        .bind(input.expected_project_version)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.updated_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id) =
            match (input.principal_type.clone(), input.principal_id.clone()) {
                (Some(kind), Some(id)) => (kind, Some(id)),
                (Some(kind), None) => (kind, None),
                (None, Some(id)) => ("system".to_owned(), Some(id)),
                (None, None) => ("system".to_owned(), None),
            };
        DomainEventRepo::append_event_in_tx(
            self,
            &mut tx,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "project.decision.candidate_created".to_owned(),
                entity_type: "project_decision_candidate".to_owned(),
                entity_id: input.id.clone(),
                actor_type,
                actor_id,
                scope_type: "project".to_owned(),
                scope_id: input.project_id.clone(),
                correlation_id: input.id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("project-decision-candidate-created:{}", input.id)),
                payload_json: serde_json::json!({
                    "project_id": input.project_id.clone(),
                    "candidate_id": input.id.clone(),
                    "lifecycle": input.lifecycle.clone(),
                })
                .to_string(),
                created_at: input.created_at.clone(),
            },
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_decision_candidate WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_decision_candidate(row)
    }

    async fn get_project_decision_candidate(
        &self,
        id: &str,
    ) -> Result<Option<ProjectDecisionCandidateRecord>> {
        select_one(
            "SELECT * FROM project_decision_candidate WHERE id = ?",
            self.pool(),
            id,
            map_decision_candidate,
        )
        .await
    }

    async fn list_project_decision_candidates(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectDecisionCandidateRecord>> {
        sqlx::query(
            "SELECT * FROM project_decision_candidate
             WHERE project_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_decision_candidate)
        .collect()
    }

    async fn append_project_decision(
        &self,
        input: CreateProjectDecision,
    ) -> Result<ProjectDecisionRecord> {
        if input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authority_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        if let Some(supersedes_id) = input.supersedes_decision_id.as_deref() {
            let belongs: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_decision
                 WHERE id = ? AND project_id = ? LIMIT 1",
            )
            .bind(supersedes_id)
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?;
            if belongs.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        sqlx::query(
            "INSERT INTO project_decision (
                id, project_id, state, decision_class, question, context_json,
                options_json, selected_outcome, rationale, principal_type,
                principal_id, authority_basis, authorization_action, explicit_event,
                authorization_occurred_at, charter_revision_id, baseline_revision_id,
                source_refs_json, affected_records_json, supersedes_decision_id,
                created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.state)
        .bind(&input.decision_class)
        .bind(&input.question)
        .bind(&input.context_json)
        .bind(&input.options_json)
        .bind(&input.selected_outcome)
        .bind(&input.rationale)
        .bind(&input.principal_type)
        .bind(&input.principal_id)
        .bind(&input.authority_basis)
        .bind(&input.authorization_action)
        .bind(&input.explicit_event)
        .bind(&input.authorization_occurred_at)
        .bind(input.charter_revision_id.as_deref())
        .bind(input.baseline_revision_id.as_deref())
        .bind(&input.source_refs_json)
        .bind(&input.affected_records_json)
        .bind(input.supersedes_decision_id.as_deref())
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.created_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        DomainEventRepo::append_event_in_tx(
            self,
            &mut tx,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "project.decision.created".to_owned(),
                entity_type: "project_decision".to_owned(),
                entity_id: input.id.clone(),
                actor_type: input.principal_type.clone(),
                actor_id: Some(input.principal_id.clone()),
                scope_type: "project".to_owned(),
                scope_id: input.project_id.clone(),
                correlation_id: input.id.clone(),
                causation_id: Some(input.explicit_event.clone()),
                causation_depth: 0,
                dedupe_key: Some(format!("project-decision-created:{}", input.id)),
                payload_json: serde_json::json!({
                    "project_id": input.project_id.clone(),
                    "decision_id": input.id.clone(),
                    "state": input.state.clone(),
                    "decision_class": input.decision_class.clone(),
                    "supersedes_decision_id": input.supersedes_decision_id.clone(),
                    "charter_revision_id": input.charter_revision_id.clone(),
                    "baseline_revision_id": input.baseline_revision_id.clone(),
                })
                .to_string(),
                created_at: input.created_at.clone(),
            },
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_decision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_decision(row)
    }

    async fn list_effective_project_decisions(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectDecisionRecord>> {
        sqlx::query(
            "SELECT * FROM project_decision
             WHERE project_id = ? AND state = 'active'
             ORDER BY created_at ASC, id ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_decision)
        .collect()
    }

    async fn create_project_execution_baseline(
        &self,
        input: CreateProjectExecutionBaseline,
    ) -> Result<ProjectExecutionBaselineRecord> {
        // The shell primary key is server-minted (matching the Charter
        // precedent): agent payloads routinely fabricate identifiers, so a
        // caller-supplied id is never trusted as a primary key.
        let id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO project_execution_baseline (
                id, project_id, current_revision_id, lifecycle, version,
                created_at, updated_at
             ) VALUES (?, ?, NULL, 'draft', 1, ?, ?)",
        )
        .bind(&id)
        .bind(&input.project_id)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(self.pool())
        .await
        .map_err(check_error)?;
        self.get_project_execution_baseline(&id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_project_execution_baseline(
        &self,
        id: &str,
    ) -> Result<Option<ProjectExecutionBaselineRecord>> {
        select_one(
            "SELECT * FROM project_execution_baseline WHERE id = ?",
            self.pool(),
            id,
            map_baseline,
        )
        .await
    }

    async fn create_project_execution_baseline_revision(
        &self,
        input: CreateProjectExecutionBaselineRevision,
    ) -> Result<ProjectExecutionBaselineRevisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let baseline = sqlx::query(
            "SELECT version, current_revision_id
             FROM project_execution_baseline WHERE id = ?",
        )
        .bind(&input.baseline_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let baseline_version: i64 = baseline.try_get("version")?;
        if baseline_version != input.expected_baseline_version {
            return Err(DbError::VersionConflict);
        }
        let current_revision_id: Option<String> = baseline.try_get("current_revision_id")?;
        if input.base_revision > 0 {
            let Some(current_revision_id) = current_revision_id else {
                return Err(DbError::VersionConflict);
            };
            let Some(base_revision_id) = input.base_revision_id.as_deref() else {
                return Err(DbError::VersionConflict);
            };
            if current_revision_id != base_revision_id {
                return Err(DbError::VersionConflict);
            }
            let base_matches: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_execution_baseline_revision
                 WHERE id = ? AND baseline_id = ? AND revision = ? LIMIT 1",
            )
            .bind(base_revision_id)
            .bind(&input.baseline_id)
            .bind(input.base_revision)
            .fetch_optional(&mut *tx)
            .await?;
            if base_matches.is_none() {
                return Err(DbError::VersionConflict);
            }
        } else if input.base_revision_id.is_some() || current_revision_id.is_some() {
            return Err(DbError::VersionConflict);
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_execution_baseline_revision WHERE baseline_id = ?",
        )
        .bind(&input.baseline_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO project_execution_baseline_revision (
                id, baseline_id, revision, base_revision, base_revision_id, lifecycle,
                charter_revision_id, document_revisions_json, plan_items_json,
                milestone_id, milestone_ids_json, milestone_definition_revision_ids_json,
                primary_milestone_id, release_policy_json,
                release_policy_revision, release_policy_digest,
                acceptance_matrix_json, capability_classes_json, risk_classes_json,
                adaptive_envelope_json, elevated_operations_json, exclusions_json,
                rollback_recovery_json, schema_version, render_version,
                rendered_view, content_digest, rendered_digest, source_refs_json, created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&input.id)
        .bind(&input.baseline_id)
        .bind(revision)
        .bind(input.base_revision)
        .bind(input.base_revision_id.as_deref())
        .bind(&input.lifecycle)
        .bind(&input.charter_revision_id)
        .bind(&input.document_revisions_json)
        .bind(&input.plan_items_json)
        .bind(input.milestone_id.as_deref())
        .bind(&input.milestone_ids_json)
        .bind(&input.milestone_definition_revision_ids_json)
        .bind(input.primary_milestone_id.as_deref())
        .bind(&input.release_policy_json)
        .bind(&input.release_policy_revision)
        .bind(&input.release_policy_digest)
        .bind(&input.acceptance_matrix_json)
        .bind(&input.capability_classes_json)
        .bind(&input.risk_classes_json)
        .bind(&input.adaptive_envelope_json)
        .bind(&input.elevated_operations_json)
        .bind(&input.exclusions_json)
        .bind(&input.rollback_recovery_json)
        .bind(&input.schema_version)
        .bind(&input.render_version)
        .bind(&input.rendered_view)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(&input.source_refs_json)
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = sqlx::query(
            "UPDATE project_execution_baseline
             SET current_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.id)
        .bind(&input.created_at)
        .bind(&input.baseline_id)
        .bind(input.expected_baseline_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_execution_baseline_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_baseline_revision(row)
    }

    async fn get_project_execution_baseline_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectExecutionBaselineRevisionRecord>> {
        select_one(
            "SELECT * FROM project_execution_baseline_revision WHERE id = ?",
            self.pool(),
            id,
            map_baseline_revision,
        )
        .await
    }

    async fn approve_project_execution_baseline(
        &self,
        input: ApproveProjectExecutionBaseline,
    ) -> Result<ProjectExecutionBaselineApprovalRecord> {
        if input.principal_type != "user"
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) = sqlx::query(
            "SELECT * FROM project_execution_baseline_approval
             WHERE idempotency_key = ?",
        )
        .bind(&input.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        .map(map_baseline_approval)
        .transpose()?
        {
            if existing.id != input.id
                || existing.baseline_id != input.baseline_id
                || existing.revision_id != input.revision_id
                || existing.expected_project_version != input.expected_project_version
                || existing.content_digest != input.content_digest
                || existing.rendered_digest != input.rendered_digest
                || existing.principal_type != input.principal_type
                || existing.principal_id != input.principal_id
                || existing.authorization_basis != input.authorization_basis
                || existing.authorization_action != input.authorization_action
                || existing.authorization_occurred_at != input.authorization_occurred_at
                || existing.explicit_event != input.explicit_event
            {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let target = sqlx::query(
            "SELECT lifecycle FROM project_execution_baseline_revision
             WHERE id = ? AND baseline_id = ? AND content_digest = ?
               AND rendered_digest = ?",
        )
        .bind(&input.revision_id)
        .bind(&input.baseline_id)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let target_lifecycle: String = target.try_get("lifecycle")?;
        if matches!(target_lifecycle.as_str(), "revoked" | "superseded") {
            return Err(DbError::VersionConflict);
        }
        let baseline = sqlx::query(
            "SELECT version, lifecycle, project_id FROM project_execution_baseline
             WHERE id = ?",
        )
        .bind(&input.baseline_id)
        .fetch_one(&mut *tx)
        .await?;
        let baseline_version: i64 = baseline.try_get("version")?;
        if baseline_version != input.expected_baseline_version {
            return Err(DbError::VersionConflict);
        }
        let baseline_project_id: String = baseline.try_get("project_id")?;
        let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(&baseline_project_id)
            .fetch_one(&mut *tx)
            .await?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "UPDATE project_execution_baseline_revision SET lifecycle = 'superseded'
             WHERE baseline_id = ? AND lifecycle = 'approved' AND id != ?",
        )
        .bind(&input.baseline_id)
        .bind(&input.revision_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let approved = sqlx::query(
            "UPDATE project_execution_baseline_revision SET lifecycle = 'approved'
             WHERE id = ? AND baseline_id = ? AND lifecycle != 'approved'",
        )
        .bind(&input.revision_id)
        .bind(&input.baseline_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if approved.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let baseline_approved = sqlx::query(
            "UPDATE project_execution_baseline
             SET lifecycle = 'approved', current_revision_id = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle IN ('draft', 'proposed', 'approved')",
        )
        .bind(&input.revision_id)
        .bind(&input.updated_at)
        .bind(&input.baseline_id)
        .bind(input.expected_baseline_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if baseline_approved.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_execution_baseline_approval (
                id, baseline_id, revision_id, expected_project_version,
                principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, content_digest, rendered_digest,
                lifecycle, idempotency_key, created_at,
                updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.baseline_id)
        .bind(&input.revision_id)
        .bind(input.expected_project_version)
        .bind(&input.principal_type)
        .bind(&input.principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.explicit_event)
        .bind(&input.authorization_occurred_at)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(&input.idempotency_key)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let row = sqlx::query("SELECT * FROM project_execution_baseline_approval WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_baseline_approval(row)
    }

    async fn activate_project_execution_baseline(
        &self,
        input: ActivateProjectExecutionBaseline,
    ) -> Result<ProjectExecutionBaselineRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let approval = sqlx::query(
            "SELECT a.baseline_id, a.revision_id, a.content_digest,
                    a.rendered_digest, a.lifecycle, b.version, b.project_id
             FROM project_execution_baseline_approval a
             JOIN project_execution_baseline b ON b.id = a.baseline_id
             WHERE a.id = ?",
        )
        .bind(&input.approval_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let lifecycle: String = approval.try_get("lifecycle")?;
        if lifecycle != "active" {
            if lifecycle == "consumed" {
                let baseline_id: String = approval.try_get("baseline_id")?;
                tx.commit().await?;
                return self
                    .get_project_execution_baseline(&baseline_id)
                    .await?
                    .ok_or(DbError::NotFound);
            }
            return Err(DbError::VersionConflict);
        }
        let baseline_id: String = approval.try_get("baseline_id")?;
        let revision_id: String = approval.try_get("revision_id")?;
        let project_id: String = approval.try_get("project_id")?;
        let baseline_version: i64 = approval.try_get("version")?;
        if baseline_version != input.expected_baseline_version {
            return Err(DbError::VersionConflict);
        }
        let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(&project_id)
            .fetch_one(&mut *tx)
            .await?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        let exact: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_execution_baseline_revision
             WHERE id = ? AND baseline_id = ?
               AND content_digest = (SELECT content_digest FROM project_execution_baseline_approval WHERE id = ?)
               AND rendered_digest = (SELECT rendered_digest FROM project_execution_baseline_approval WHERE id = ?)",
        )
        .bind(&revision_id)
        .bind(&baseline_id)
        .bind(&input.approval_id)
        .bind(&input.approval_id)
        .fetch_optional(&mut *tx)
        .await?;
        if exact.is_none() {
            return Err(DbError::VersionConflict);
        }
        let baseline_revision = sqlx::query(
            "SELECT milestone_id, milestone_ids_json, primary_milestone_id
             FROM project_execution_baseline_revision
             WHERE id = ? AND baseline_id = ?",
        )
        .bind(&revision_id)
        .bind(&baseline_id)
        .fetch_one(&mut *tx)
        .await?;
        let mut included_milestone_ids: Vec<String> =
            serde_json::from_str(&baseline_revision.try_get::<String, _>("milestone_ids_json")?)
                .map_err(|error| {
                    DbError::Check(format!("invalid baseline milestone ids: {error}"))
                })?;
        if let Some(milestone_id) =
            baseline_revision.try_get::<Option<String>, _>("milestone_id")?
        {
            if !included_milestone_ids.iter().any(|id| id == &milestone_id) {
                included_milestone_ids.push(milestone_id);
            }
        }
        let primary_milestone_id =
            baseline_revision.try_get::<Option<String>, _>("primary_milestone_id")?;
        if let Some(primary_milestone_id) = primary_milestone_id.as_deref() {
            if !included_milestone_ids
                .iter()
                .any(|id| id == primary_milestone_id)
            {
                return Err(DbError::VersionConflict);
            }
        }
        // A baseline activation is the lifecycle gate for its included
        // milestones. Validate every reference before changing any row so a
        // cross-Project, cancelled, or otherwise invalid milestone cannot
        // leave a partially activated baseline.
        for milestone_id in &included_milestone_ids {
            let milestone =
                sqlx::query("SELECT project_id, lifecycle FROM project_milestone WHERE id = ?")
                    .bind(milestone_id)
                    .fetch_optional(&mut *tx)
                    .await?
                    .ok_or(DbError::VersionConflict)?;
            let milestone_project_id: String = milestone.try_get("project_id")?;
            let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
            if milestone_project_id != project_id
                || !matches!(
                    milestone_lifecycle.as_str(),
                    "planned" | "active" | "ready_for_release" | "released"
                )
            {
                return Err(DbError::VersionConflict);
            }
        }
        let prior_active: Option<String> = sqlx::query_scalar(
            "SELECT id FROM project_execution_baseline
             WHERE project_id = ? AND lifecycle = 'active' AND id != ? LIMIT 1",
        )
        .bind(&project_id)
        .bind(&baseline_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(prior_active) = prior_active {
            let superseded = sqlx::query(
                "UPDATE project_execution_baseline SET lifecycle = 'superseded',
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND lifecycle = 'active'",
            )
            .bind(&input.updated_at)
            .bind(prior_active)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?;
            if superseded.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
        let active = sqlx::query(
            "UPDATE project_execution_baseline SET lifecycle = 'active',
                 current_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle IN ('draft', 'proposed', 'approved')",
        )
        .bind(&revision_id)
        .bind(&input.updated_at)
        .bind(&baseline_id)
        .bind(input.expected_baseline_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if active.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        for milestone_id in &included_milestone_ids {
            let activated = sqlx::query(
                "UPDATE project_milestone
                 SET lifecycle = 'active', version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND lifecycle = 'planned'",
            )
            .bind(&input.updated_at)
            .bind(milestone_id)
            .bind(&project_id)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?;
            if activated.rows_affected() == 0 {
                let lifecycle: String = sqlx::query_scalar(
                    "SELECT lifecycle FROM project_milestone
                     WHERE id = ? AND project_id = ?",
                )
                .bind(milestone_id)
                .bind(&project_id)
                .fetch_one(&mut *tx)
                .await?;
                if !matches!(
                    lifecycle.as_str(),
                    "active" | "ready_for_release" | "released"
                ) {
                    return Err(DbError::VersionConflict);
                }
            }
        }
        let project_update = sqlx::query(
            "UPDATE project SET primary_milestone_id = COALESCE(?, primary_milestone_id),
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(primary_milestone_id.as_deref())
        .bind(&input.updated_at)
        .bind(&project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if project_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        // Promote matching preplanned Tasks in the same transaction: flip
        // rows already bound to this exact revision and re-bind non-terminal
        // Tasks whose plan item this baseline covers.
        crate::promote_baseline_task_governance_in_tx(
            &mut tx,
            &project_id,
            &baseline_id,
            &revision_id,
            &input.updated_at,
        )
        .await?;
        let consumed = sqlx::query(
            "UPDATE project_execution_baseline_approval
             SET lifecycle = 'consumed', updated_at = ?
             WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(&input.updated_at)
        .bind(&input.approval_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if consumed.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_execution_baseline WHERE id = ?")
            .bind(&baseline_id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_baseline(row)
    }

    async fn create_project_milestone(
        &self,
        input: CreateProjectMilestone,
    ) -> Result<ProjectMilestoneRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_milestone (
                id, project_id, milestone_sequence, milestone_key, display_label,
                lifecycle, blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'planned', '[]', '[]', '[]', 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(input.milestone_sequence)
        .bind(&input.milestone_key)
        .bind(input.display_label.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.updated_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_milestone WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_milestone(row)
    }

    async fn list_project_milestones(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectMilestoneRecord>> {
        sqlx::query(
            "SELECT * FROM project_milestone
             WHERE project_id = ? ORDER BY milestone_sequence ASC, id ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_milestone)
        .collect()
    }

    async fn get_project_milestone(&self, id: &str) -> Result<Option<ProjectMilestoneRecord>> {
        select_one(
            "SELECT * FROM project_milestone WHERE id = ?",
            self.pool(),
            id,
            map_milestone,
        )
        .await
    }

    async fn create_project_milestone_revision(
        &self,
        input: CreateProjectMilestoneRevision,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let milestone = sqlx::query(
            "SELECT version, current_definition_revision_id
             FROM project_milestone WHERE id = ?",
        )
        .bind(&input.milestone_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let milestone_version: i64 = milestone.try_get("version")?;
        if milestone_version != input.expected_milestone_version {
            return Err(DbError::VersionConflict);
        }
        let current_revision_id: Option<String> =
            milestone.try_get("current_definition_revision_id")?;
        if input.base_revision > 0 {
            let Some(current_revision_id) = current_revision_id else {
                return Err(DbError::VersionConflict);
            };
            let Some(base_revision_id) = input.base_revision_id.as_deref() else {
                return Err(DbError::VersionConflict);
            };
            if current_revision_id != base_revision_id {
                return Err(DbError::VersionConflict);
            }
            let base_matches: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_milestone_revision
                 WHERE id = ? AND milestone_id = ? AND revision = ? LIMIT 1",
            )
            .bind(base_revision_id)
            .bind(&input.milestone_id)
            .bind(input.base_revision)
            .fetch_optional(&mut *tx)
            .await?;
            if base_matches.is_none() {
                return Err(DbError::VersionConflict);
            }
        } else if input.base_revision_id.is_some() || current_revision_id.is_some() {
            return Err(DbError::VersionConflict);
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_milestone_revision WHERE milestone_id = ?",
        )
        .bind(&input.milestone_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO project_milestone_revision (
                id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
                display_label, outcome, included_scope_json, excluded_scope_json,
                charter_revision_id, document_revisions_json, task_selection_json,
                dependencies_json, risks_json, acceptance_checks_json,
                evidence_requirements_json, known_issues_json, change_summary,
                schema_version, render_version, rendered_view, content_digest,
                rendered_digest,
                author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.milestone_id)
        .bind(revision)
        .bind(input.base_revision)
        .bind(input.base_revision_id.as_deref())
        .bind(&input.lifecycle)
        .bind(input.display_label.as_deref())
        .bind(&input.outcome)
        .bind(&input.included_scope_json)
        .bind(&input.excluded_scope_json)
        .bind(input.charter_revision_id.as_deref())
        .bind(&input.document_revisions_json)
        .bind(&input.task_selection_json)
        .bind(&input.dependencies_json)
        .bind(&input.risks_json)
        .bind(&input.acceptance_checks_json)
        .bind(&input.evidence_requirements_json)
        .bind(&input.known_issues_json)
        .bind(&input.change_summary)
        .bind(&input.schema_version)
        .bind(&input.render_version)
        .bind(&input.rendered_view)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(&input.author_type)
        .bind(input.author_id.as_deref())
        .bind(&input.source_refs_json)
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = if input.lifecycle == "draft" {
            sqlx::query(
                "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ?",
            )
            .bind(&input.created_at)
            .bind(&input.milestone_id)
            .bind(input.expected_milestone_version)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?
        } else {
            sqlx::query(
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?, version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = ?",
            )
            .bind(&input.id)
            .bind(&input.created_at)
            .bind(&input.milestone_id)
            .bind(input.expected_milestone_version)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?
        };
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_milestone_revision(row)
    }

    async fn create_project_milestone_atomically(
        &self,
        input: CreateProjectMilestoneAtomically,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        if input.milestone.id != input.revision.milestone_id
            || input.revision.expected_milestone_version != 1
            || input.revision.base_revision != 0
            || input.revision.base_revision_id.is_some()
        {
            return Err(DbError::VersionConflict);
        }

        let mut tx = crate::begin_immediate(self.pool()).await?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&input.milestone.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.milestone.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_milestone (
                id, project_id, milestone_sequence, milestone_key, display_label,
                lifecycle, blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'planned', '[]', '[]', '[]', 1, ?, ?)",
        )
        .bind(&input.milestone.id)
        .bind(&input.milestone.project_id)
        .bind(input.milestone.milestone_sequence)
        .bind(&input.milestone.milestone_key)
        .bind(input.milestone.display_label.as_deref())
        .bind(&input.milestone.created_at)
        .bind(&input.milestone.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.milestone.updated_at)
        .bind(&input.milestone.project_id)
        .bind(input.milestone.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_milestone_revision (
                id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
                display_label, outcome, included_scope_json, excluded_scope_json,
                charter_revision_id, document_revisions_json, task_selection_json,
                dependencies_json, risks_json, acceptance_checks_json,
                evidence_requirements_json, known_issues_json, change_summary,
                schema_version, render_version, rendered_view, content_digest,
                rendered_digest,
                author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, 1, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.milestone_id)
        .bind(&input.revision.lifecycle)
        .bind(input.revision.display_label.as_deref())
        .bind(&input.revision.outcome)
        .bind(&input.revision.included_scope_json)
        .bind(&input.revision.excluded_scope_json)
        .bind(input.revision.charter_revision_id.as_deref())
        .bind(&input.revision.document_revisions_json)
        .bind(&input.revision.task_selection_json)
        .bind(&input.revision.dependencies_json)
        .bind(&input.revision.risks_json)
        .bind(&input.revision.acceptance_checks_json)
        .bind(&input.revision.evidence_requirements_json)
        .bind(&input.revision.known_issues_json)
        .bind(&input.revision.change_summary)
        .bind(&input.revision.schema_version)
        .bind(&input.revision.render_version)
        .bind(&input.revision.rendered_view)
        .bind(&input.revision.content_digest)
        .bind(&input.revision.rendered_digest)
        .bind(&input.revision.author_type)
        .bind(input.revision.author_id.as_deref())
        .bind(&input.revision.source_refs_json)
        .bind(&input.revision.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        // The pointer trigger only accepts a 'proposed'/'approved' target, so
        // a 'draft' first revision must skip the pointer update here exactly
        // like `create_project_milestone_revision` does -- the pointer is
        // promoted by a later revision, not by this creation.
        let advanced = if input.revision.lifecycle == "draft" {
            sqlx::query(
                "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND version = 1",
            )
            .bind(&input.revision.created_at)
            .bind(&input.milestone.id)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?
        } else {
            sqlx::query(
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?, version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = 1",
            )
            .bind(&input.revision.id)
            .bind(&input.revision.created_at)
            .bind(&input.milestone.id)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?
        };
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
            .bind(&input.revision.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_milestone_revision(row)
    }

    async fn get_project_milestone_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectMilestoneRevisionRecord>> {
        select_one(
            "SELECT * FROM project_milestone_revision WHERE id = ?",
            self.pool(),
            id,
            map_milestone_revision,
        )
        .await
    }

    async fn list_project_milestone_revisions(
        &self,
        milestone_id: &str,
    ) -> Result<Vec<ProjectMilestoneRevisionRecord>> {
        sqlx::query(
            "SELECT * FROM project_milestone_revision
             WHERE milestone_id = ? ORDER BY revision ASC, id ASC",
        )
        .bind(milestone_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_milestone_revision)
        .collect()
    }

    async fn create_project_milestone_check(
        &self,
        input: CreateProjectMilestoneCheck,
    ) -> Result<ProjectMilestoneCheckRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let milestone = sqlx::query(
            "SELECT version FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let milestone_version: i64 = milestone.try_get("version")?;
        if milestone_version != input.expected_milestone_version {
            return Err(DbError::VersionConflict);
        }
        let definition_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone_revision
             WHERE id = ? AND milestone_id = ? LIMIT 1",
        )
        .bind(&input.definition_revision_id)
        .bind(&input.milestone_id)
        .fetch_optional(&mut *tx)
        .await?;
        if definition_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_milestone_check (
                id, project_id, milestone_id, definition_revision_id, check_key,
                description, required, source_kind, expected_result,
                evidence_required, version, current_result_id, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(&input.definition_revision_id)
        .bind(&input.check_key)
        .bind(&input.description)
        .bind(input.required)
        .bind(&input.source_kind)
        .bind(&input.expected_result)
        .bind(input.evidence_required)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = sqlx::query(
            "UPDATE project_milestone SET version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(&input.updated_at)
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .bind(input.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_milestone_check WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_milestone_check(row)
    }

    async fn append_project_milestone_check_result(
        &self,
        input: CreateProjectMilestoneCheckResult,
    ) -> Result<ProjectMilestoneCheckResultRecord> {
        if input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) =
            sqlx::query("SELECT * FROM project_milestone_check_result WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut *tx)
                .await?
                .map(map_milestone_result)
                .transpose()?
        {
            if existing.id != input.id
                || existing.project_id != input.project_id
                || existing.milestone_id != input.milestone_id
                || existing.check_id != input.check_id
                || existing.definition_revision_id != input.definition_revision_id
                || existing.source_kind != input.source_kind
                || existing.source_manifest_json != input.source_manifest_json
                || existing.input_digest != input.input_digest
                || existing.outcome != input.outcome
                || existing.governing_charter_revision_id != input.governing_charter_revision_id
                || existing.governing_baseline_revision_id != input.governing_baseline_revision_id
                || existing.principal_type != input.principal_type
                || existing.principal_id != input.principal_id
                || existing.authorization_basis != input.authorization_basis
                || existing.authorization_action != input.authorization_action
                || existing.authorization_occurred_at != input.authorization_occurred_at
                || existing.expected_version != input.expected_version
                || existing.explicit_event != input.explicit_event
            {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let check = sqlx::query(
            "SELECT version FROM project_milestone_check
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND definition_revision_id = ? AND source_kind = ?",
        )
        .bind(&input.check_id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(&input.definition_revision_id)
        .bind(&input.source_kind)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let check_version: i64 = check.try_get("version")?;
        if check_version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_milestone_check_result (
                id, project_id, milestone_id, check_id, definition_revision_id,
                outcome, source_kind, source_manifest_json, input_digest,
                governing_charter_revision_id, governing_baseline_revision_id,
                principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at, expected_version,
                explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(&input.check_id)
        .bind(&input.definition_revision_id)
        .bind(&input.outcome)
        .bind(&input.source_kind)
        .bind(&input.source_manifest_json)
        .bind(&input.input_digest)
        .bind(input.governing_charter_revision_id.as_deref())
        .bind(input.governing_baseline_revision_id.as_deref())
        .bind(&input.principal_type)
        .bind(&input.principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.authorization_occurred_at)
        .bind(input.expected_version)
        .bind(&input.explicit_event)
        .bind(&input.idempotency_key)
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let advanced = sqlx::query(
            "UPDATE project_milestone_check
             SET current_result_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.id)
        .bind(&input.created_at)
        .bind(&input.check_id)
        .bind(input.expected_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_milestone_check_result WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_milestone_result(row)
    }

    async fn create_project_readiness_snapshot(
        &self,
        input: CreateProjectReadinessSnapshot,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        if input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) =
            sqlx::query("SELECT * FROM project_readiness_snapshot WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut *tx)
                .await?
                .map(map_readiness)
                .transpose()?
        {
            if existing.id != input.id
                || existing.project_id != input.project_id
                || existing.milestone_id != input.milestone_id
                || existing.definition_revision_id != input.definition_revision_id
                || existing.baseline_id != input.baseline_id
                || existing.baseline_revision_id != input.baseline_revision_id
                || existing.baseline_digest != input.baseline_digest
                || existing.release_policy_revision != input.release_policy_revision
                || existing.release_policy_digest != input.release_policy_digest
                || existing.input_manifest_json != input.input_manifest_json
                || existing.event_watermark != input.event_watermark
                || existing.outcome != input.outcome
                || existing.blocking_reasons_json != input.blocking_reasons_json
                || existing.check_results_json != input.check_results_json
                || existing.waiver_manifest_json != input.waiver_manifest_json
                || existing.evidence_manifest_json != input.evidence_manifest_json
                || existing.commit_context_json != input.commit_context_json
                || existing.computing_policy_revision != input.computing_policy_revision
                || existing.readiness_digest != input.readiness_digest
                || existing.principal_type != input.principal_type
                || existing.principal_id != input.principal_id
                || existing.authorization_basis != input.authorization_basis
                || existing.authorization_action != input.authorization_action
                || existing.authorization_occurred_at != input.authorization_occurred_at
                || existing.expected_milestone_version != input.expected_milestone_version
                || existing.explicit_event != input.explicit_event
            {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let milestone = sqlx::query(
            "SELECT version, lifecycle FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let milestone_version: i64 = milestone.try_get("version")?;
        let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
        if milestone_version != input.expected_milestone_version {
            return Err(DbError::VersionConflict);
        }
        let definition_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone_revision
             WHERE id = ? AND milestone_id = ? LIMIT 1",
        )
        .bind(&input.definition_revision_id)
        .bind(&input.milestone_id)
        .fetch_optional(&mut *tx)
        .await?;
        if definition_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
        let baseline_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM project_execution_baseline b
             JOIN project_execution_baseline_revision r
               ON r.id = ? AND r.baseline_id = b.id
             WHERE b.id = ? AND b.project_id = ?
               AND b.lifecycle = 'active'
               AND b.current_revision_id = r.id
               AND r.lifecycle = 'approved'
               AND r.content_digest = ?
               AND r.release_policy_revision = ?
               AND r.release_policy_digest = ?
             LIMIT 1",
        )
        .bind(&input.baseline_revision_id)
        .bind(&input.baseline_id)
        .bind(&input.project_id)
        .bind(&input.baseline_digest)
        .bind(&input.release_policy_revision)
        .bind(&input.release_policy_digest)
        .fetch_optional(&mut *tx)
        .await?;
        if baseline_matches.is_none()
            || input.release_policy_revision.trim().is_empty()
            || input.release_policy_digest.trim().is_empty()
        {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_readiness_snapshot (
                id, project_id, milestone_id, definition_revision_id, baseline_id,
                baseline_revision_id, baseline_digest, release_policy_revision,
                release_policy_digest, input_manifest_json, event_watermark, outcome,
                blocking_reasons_json, check_results_json,
                waiver_manifest_json, evidence_manifest_json, commit_context_json,
                computing_policy_revision, readiness_digest, principal_type,
                principal_id, authorization_basis, authorization_action,
                authorization_occurred_at, expected_milestone_version,
                explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(&input.definition_revision_id)
        .bind(&input.baseline_id)
        .bind(&input.baseline_revision_id)
        .bind(&input.baseline_digest)
        .bind(&input.release_policy_revision)
        .bind(&input.release_policy_digest)
        .bind(&input.input_manifest_json)
        .bind(&input.event_watermark)
        .bind(&input.outcome)
        .bind(&input.blocking_reasons_json)
        .bind(&input.check_results_json)
        .bind(&input.waiver_manifest_json)
        .bind(&input.evidence_manifest_json)
        .bind(&input.commit_context_json)
        .bind(&input.computing_policy_revision)
        .bind(&input.readiness_digest)
        .bind(&input.principal_type)
        .bind(&input.principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.authorization_occurred_at)
        .bind(input.expected_milestone_version)
        .bind(&input.explicit_event)
        .bind(&input.idempotency_key)
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if milestone_lifecycle != "released" {
            let lifecycle = if input.outcome == "ready" {
                "ready_for_release"
            } else {
                "active"
            };
            let updated = sqlx::query(
                "UPDATE project_milestone
                 SET lifecycle = ?, blocker_reason_json = ?, stale_reason_json = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
            )
            .bind(lifecycle)
            .bind(&input.blocking_reasons_json)
            .bind(if input.outcome == "stale" {
                &input.blocking_reasons_json
            } else {
                "[]"
            })
            .bind(&input.created_at)
            .bind(&input.milestone_id)
            .bind(&input.project_id)
            .bind(input.expected_milestone_version)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?;
            if updated.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
        let row = sqlx::query("SELECT * FROM project_readiness_snapshot WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_readiness(row)
    }

    async fn create_project_release(
        &self,
        input: CreateProjectRelease,
        references: Vec<CreateProjectReleaseReference>,
    ) -> Result<ProjectReleaseRecord> {
        if input.releasing_principal_type.trim().is_empty()
            || input.releasing_principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) =
            sqlx::query("SELECT * FROM project_release WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut *tx)
                .await?
                .map(map_release)
                .transpose()?
        {
            if existing.id != input.id
                || existing.project_id != input.project_id
                || existing.milestone_id != input.milestone_id
                || existing.release_sequence != input.release_sequence
                || existing.release_revision != input.release_revision
                || existing.release_identifier != input.release_identifier
                || existing.milestone_revision_id != input.milestone_revision_id
                || existing.readiness_snapshot_id != input.readiness_snapshot_id
                || existing.readiness_digest != input.readiness_digest
                || existing.baseline_id != input.baseline_id
                || existing.baseline_revision_id != input.baseline_revision_id
                || existing.baseline_digest != input.baseline_digest
                || existing.release_policy_revision != input.release_policy_revision
                || existing.release_policy_digest != input.release_policy_digest
                || existing.summary != input.summary
                || existing.changelog != input.changelog
                || existing.known_issues_json != input.known_issues_json
                || existing.charter_revision_id != input.charter_revision_id
                || existing.document_revisions_json != input.document_revisions_json
                || existing.decision_ids_json != input.decision_ids_json
                || existing.task_references_json != input.task_references_json
                || existing.validation_references_json != input.validation_references_json
                || existing.git_references_json != input.git_references_json
                || existing.evidence_references_json != input.evidence_references_json
                || existing.waivers_json != input.waivers_json
                || existing.releasing_principal_type != input.releasing_principal_type
                || existing.releasing_principal_id != input.releasing_principal_id
                || existing.authorization_basis != input.authorization_basis
                || existing.authorization_action != input.authorization_action
                || existing.authorization_occurred_at != input.authorization_occurred_at
                || existing.explicit_event != input.explicit_event
                || existing.schema_version != input.schema_version
                || existing.snapshot_digest != input.snapshot_digest
            {
                return Err(DbError::VersionConflict);
            }
            let persisted_references = sqlx::query(
                "SELECT ordinal, reference_kind, record_id, record_version,
                        record_state, record_digest, metadata_json
                 FROM project_release_reference
                 WHERE release_id = ? ORDER BY ordinal ASC",
            )
            .bind(&existing.id)
            .fetch_all(&mut *tx)
            .await?;
            let same_references = persisted_references.len() == references.len()
                && persisted_references
                    .iter()
                    .zip(references.iter())
                    .all(|(row, reference)| {
                        row.try_get::<i64, _>("ordinal").ok() == Some(reference.ordinal)
                            && row.try_get::<String, _>("reference_kind").ok()
                                == Some(reference.reference_kind.clone())
                            && row.try_get::<String, _>("record_id").ok()
                                == Some(reference.record_id.clone())
                            && row.try_get::<Option<String>, _>("record_version").ok()
                                == Some(reference.record_version.clone())
                            && row.try_get::<Option<String>, _>("record_state").ok()
                                == Some(reference.record_state.clone())
                            && row.try_get::<Option<String>, _>("record_digest").ok()
                                == Some(reference.record_digest.clone())
                            && row.try_get::<String, _>("metadata_json").ok()
                                == Some(reference.metadata_json.clone())
                    });
            if !same_references {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let milestone = sqlx::query(
            "SELECT version, lifecycle, milestone_key FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let milestone_version: i64 = milestone.try_get("version")?;
        if milestone_version != input.expected_milestone_version {
            return Err(DbError::VersionConflict);
        }
        let readiness = sqlx::query(
            "SELECT definition_revision_id, baseline_id, baseline_revision_id,
                    baseline_digest, release_policy_revision, release_policy_digest
             FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND readiness_digest = ? AND outcome = 'ready'",
        )
        .bind(&input.readiness_snapshot_id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(&input.readiness_digest)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let definition_revision_id: String = readiness.try_get("definition_revision_id")?;
        let readiness_baseline_id: String = readiness.try_get("baseline_id")?;
        let readiness_baseline_revision_id: String = readiness.try_get("baseline_revision_id")?;
        let readiness_baseline_digest: String = readiness.try_get("baseline_digest")?;
        let readiness_policy_revision: String = readiness.try_get("release_policy_revision")?;
        let readiness_policy_digest: String = readiness.try_get("release_policy_digest")?;
        if definition_revision_id != input.milestone_revision_id
            || readiness_baseline_id != input.baseline_id
            || readiness_baseline_revision_id != input.baseline_revision_id
            || readiness_baseline_digest != input.baseline_digest
            || readiness_policy_revision != input.release_policy_revision
            || readiness_policy_digest != input.release_policy_digest
        {
            return Err(DbError::VersionConflict);
        }
        let baseline_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM project_execution_baseline b
             JOIN project_execution_baseline_revision r
               ON r.id = ? AND r.baseline_id = b.id
             WHERE b.id = ? AND b.project_id = ?
               AND b.lifecycle = 'active'
               AND b.current_revision_id = r.id
               AND r.lifecycle = 'approved'
               AND r.content_digest = ?
               AND r.release_policy_revision = ?
               AND r.release_policy_digest = ?
             LIMIT 1",
        )
        .bind(&input.baseline_revision_id)
        .bind(&input.baseline_id)
        .bind(&input.project_id)
        .bind(&input.baseline_digest)
        .bind(&input.release_policy_revision)
        .bind(&input.release_policy_digest)
        .fetch_optional(&mut *tx)
        .await?;
        if baseline_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
        if let Some(charter_revision_id) = input.charter_revision_id.as_deref() {
            let charter_matches: Option<i64> = sqlx::query_scalar(
                "SELECT 1
                 FROM project_charter c
                 JOIN project_charter_revision r ON r.id = ? AND r.charter_id = c.id
                 WHERE c.project_id = ?
                   AND c.current_approved_revision_id = r.id
                   AND r.lifecycle = 'approved'
                 LIMIT 1",
            )
            .bind(charter_revision_id)
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?;
            if charter_matches.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        sqlx::query(
            "INSERT INTO project_release (
                id, project_id, milestone_id, release_sequence, release_revision,
                release_identifier, milestone_revision_id, readiness_snapshot_id,
                readiness_digest, baseline_id, baseline_revision_id, baseline_digest,
                release_policy_revision, release_policy_digest, summary, changelog,
                known_issues_json,
                charter_revision_id, document_revisions_json, decision_ids_json,
                task_references_json, validation_references_json, git_references_json,
                evidence_references_json, waivers_json, releasing_principal_type,
                releasing_principal_id, authorization_basis, authorization_action,
                authorization_occurred_at, explicit_event, schema_version,
                snapshot_digest, idempotency_key, created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(input.release_sequence)
        .bind(input.release_revision)
        .bind(&input.release_identifier)
        .bind(&input.milestone_revision_id)
        .bind(&input.readiness_snapshot_id)
        .bind(&input.readiness_digest)
        .bind(&input.baseline_id)
        .bind(&input.baseline_revision_id)
        .bind(&input.baseline_digest)
        .bind(&input.release_policy_revision)
        .bind(&input.release_policy_digest)
        .bind(&input.summary)
        .bind(&input.changelog)
        .bind(&input.known_issues_json)
        .bind(input.charter_revision_id.as_deref())
        .bind(&input.document_revisions_json)
        .bind(&input.decision_ids_json)
        .bind(&input.task_references_json)
        .bind(&input.validation_references_json)
        .bind(&input.git_references_json)
        .bind(&input.evidence_references_json)
        .bind(&input.waivers_json)
        .bind(&input.releasing_principal_type)
        .bind(&input.releasing_principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.authorization_occurred_at)
        .bind(&input.explicit_event)
        .bind(&input.schema_version)
        .bind(&input.snapshot_digest)
        .bind(&input.idempotency_key)
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            if error.to_string().to_ascii_lowercase().contains("unique") {
                DbError::VersionConflict
            } else {
                check_error(error)
            }
        })?;
        for reference in references {
            sqlx::query(
                "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&reference.release_id)
            .bind(reference.ordinal)
            .bind(&reference.reference_kind)
            .bind(&reference.record_id)
            .bind(reference.record_version.as_deref())
            .bind(reference.record_state.as_deref())
            .bind(reference.record_digest.as_deref())
            .bind(&reference.metadata_json)
            .execute(&mut *tx)
            .await
            .map_err(check_error)?;
        }
        let released = sqlx::query(
            "UPDATE project_milestone SET lifecycle = 'released', version = version + 1,
                 updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('ready_for_release', 'released')",
        )
        .bind(&input.created_at)
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .bind(input.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if released.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_release WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_release(row)
    }

    async fn list_project_release_references(
        &self,
        release_id: &str,
    ) -> Result<Vec<ProjectReleaseReferenceRecord>> {
        sqlx::query(
            "SELECT * FROM project_release_reference
             WHERE release_id = ? ORDER BY ordinal ASC",
        )
        .bind(release_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_release_reference)
        .collect()
    }

    async fn create_project_from_charter_approval(
        &self,
        input: CreateProjectFromCharterApproval,
    ) -> Result<CreatedProjectFromCharterApproval> {
        let mut tx = crate::begin_immediate(self.pool()).await?;

        // The create authorization is a second, explicit user action. Keep it
        // separate from the approval receipt and validate it again at the DB
        // boundary before any replay or mutation path can proceed.
        if input.create_principal_type != "user"
            || input.create_principal_id != input.account_id
            || input.create_action != "product_genesis.create_project_from_approval"
            || input.create_authorization_basis.trim().is_empty()
            || input.create_event_id.trim().is_empty()
            || input.create_occurred_at.trim().is_empty()
            || !valid_authorization_timestamp(&input.create_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }

        // Replay is intentionally resolved before checking the active receipt.
        // A consumed receipt is the durable idempotency record for the whole
        // composite operation, not an invitation to create a second Project.
        let approval_row = sqlx::query(
            "SELECT a.*, c.account_id, c.genesis_session_id, c.project_id AS charter_project_id,
                    c.current_approved_revision_id, a.approved_project_mode,
                    r.content_digest AS revision_content_digest,
                    r.rendered_digest AS revision_rendered_digest,
                    g.lifecycle AS genesis_lifecycle, g.project_id AS genesis_project_id,
                    g.handoff_id AS genesis_handoff_id, g.main_chat_id,
                    r.content_json AS revision_content_json
             FROM project_charter_approval a
             JOIN project_charter c ON c.id = a.charter_id
             JOIN project_charter_revision r ON r.id = a.revision_id
             LEFT JOIN product_genesis_session g ON g.id = c.genesis_session_id
             WHERE a.id = ?",
        )
        .bind(&input.approval_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;

        let approval = sqlx::query("SELECT * FROM project_charter_approval WHERE id = ?")
            .bind(&input.approval_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(DbError::from)
            .and_then(map_charter_approval)?;
        let account_id: String = approval_row.try_get("account_id")?;
        if account_id != input.account_id {
            return Err(DbError::VersionConflict);
        }
        let approval_event_id = approval
            .approval_event_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let approval_event = sqlx::query(
            "SELECT principal_type, principal_id, authorization_basis,
                    action, explicit_event, occurred_at, lifecycle
             FROM project_charter_approval_event
             WHERE id = ? AND approval_id = ?",
        )
        .bind(approval_event_id)
        .bind(&approval.id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let event_lifecycle: String = approval_event.try_get("lifecycle")?;
        let event_principal_type: String = approval_event.try_get("principal_type")?;
        let event_principal_id: String = approval_event.try_get("principal_id")?;
        let event_authorization_basis: String = approval_event.try_get("authorization_basis")?;
        let event_action: String = approval_event.try_get("action")?;
        let event_explicit_event: String = approval_event.try_get("explicit_event")?;
        let event_occurred_at: String = approval_event.try_get("occurred_at")?;
        if event_lifecycle != "active"
            || event_principal_type != approval.approving_principal_type
            || event_principal_id != approval.approving_principal_id
            || event_authorization_basis != approval.authorization_basis
            || event_action != approval.authorization_action
            || event_explicit_event != approval.explicit_event
            || event_occurred_at != approval.authorization_occurred_at
        {
            return Err(DbError::VersionConflict);
        }
        if approval.approved_name.as_deref() != Some(input.project.name.as_str())
            || approval.selected_policy_digest.as_deref() != Some(input.policy_digest.as_str())
            || approval.selected_policy_revision.as_deref() != Some(input.policy_revision.as_str())
            || input.project.owner_id.as_deref() != Some(input.account_id.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        let requested_mode = serde_json::from_str::<serde_json::Value>(&input.project.settings)
            .ok()
            .and_then(|settings| {
                settings
                    .get("project_mode")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        if requested_mode.as_deref() != Some(approval.approved_project_mode.as_str()) {
            return Err(DbError::VersionConflict);
        }
        let main_chat_id: String = approval_row.try_get("main_chat_id")?;
        if approval.lifecycle == "consumed" {
            if approval.consumed_project_id.is_none() {
                return Err(DbError::VersionConflict);
            }
            let project_id = approval
                .consumed_project_id
                .as_deref()
                .ok_or(DbError::NotFound)?;
            let project_row = sqlx::query(&format!(
                "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
            ))
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(DbError::from)?;
            let project = map_project(project_row)?;
            if project.id != input.project.id
                || project.name != input.project.name
                || project.settings != input.project.settings
                || project.workflow_definition != input.project.workflow_definition
                || project.primary_repo_id != input.project.primary_repo_id
                || project.owner_id != input.project.owner_id
            {
                return Err(DbError::VersionConflict);
            }
            let (
                project_chat_id,
                binding_id,
                binding_identity_id,
                binding_profile_id,
                binding_skill_revision_id,
                binding_policy_revision,
                binding_policy_digest,
                binding_charter_id,
                binding_charter_revision_id,
                chat_status,
                binding_state,
            ): (
                String,
                String,
                String,
                String,
                Option<String>,
                String,
                String,
                Option<String>,
                Option<String>,
                String,
                String,
            ) = sqlx::query_as(
                "SELECT c.id, b.id, b.identity_id, b.profile_id,
                        b.operating_skill_revision_id, b.policy_revision, b.policy_digest,
                        b.charter_id, b.charter_revision_id, c.status, b.state
                 FROM agent_chat c
                 JOIN project_agent_binding b ON b.project_id = c.project_id
                   AND b.state = 'active'
                 WHERE c.project_id = ? AND c.kind = 'project' AND b.id = ?
                 LIMIT 1",
            )
            .bind(project_id)
            .bind(&input.project_agent_binding_id)
            .fetch_one(&mut *tx)
            .await?;
            if chat_status != "ready"
                || binding_state != "active"
                || binding_identity_id
                    != approval
                        .selected_identity_id
                        .clone()
                        .ok_or(DbError::VersionConflict)?
                || binding_profile_id
                    != approval
                        .selected_profile_id
                        .clone()
                        .ok_or(DbError::VersionConflict)?
                || binding_skill_revision_id != approval.selected_operating_skill_revision_id
                || binding_policy_revision
                    != approval
                        .selected_policy_revision
                        .clone()
                        .ok_or(DbError::VersionConflict)?
                || binding_policy_digest != input.policy_digest
                || binding_charter_id.as_deref() != Some(approval.charter_id.as_str())
                || binding_charter_revision_id.as_deref() != Some(approval.revision_id.as_str())
            {
                return Err(DbError::VersionConflict);
            }
            let handoff = sqlx::query(
                "SELECT * FROM agent_handoff
                 WHERE target_chat_id = ? AND dedupe_key = ? LIMIT 1",
            )
            .bind(&project_chat_id)
            .bind(&input.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::VersionConflict)?;
            let handoff_id: String = handoff.try_get("id")?;
            let source_chat_id: String = handoff.try_get("source_chat_id")?;
            let stored_source_message_id: Option<String> = handoff.try_get("source_message_id")?;
            let stored_source_turn_id: Option<String> = handoff.try_get("source_turn_job_id")?;
            let target_message_id: String = handoff.try_get("target_message_id")?;
            let target_turn_id: String = handoff.try_get("target_turn_job_id")?;
            let stored_author_identity_id: Option<String> =
                handoff.try_get("author_identity_id")?;
            let stored_content: String = handoff.try_get("content")?;
            let stored_content_guard: String = handoff.try_get("content_guard_json")?;
            let stored_source_revisions: String = handoff.try_get("source_revisions_json")?;
            let stored_correlation_id: String = handoff.try_get("correlation_id")?;
            let stored_causation_id: Option<String> = handoff.try_get("causation_id")?;
            if handoff_id != input.handoff_id
                || source_chat_id != main_chat_id
                || stored_source_message_id != input.source_message_id
                || stored_source_turn_id != input.source_turn_id
                || target_message_id != input.target_message_id
                || target_turn_id != input.target_turn_id
                || stored_author_identity_id.as_deref()
                    != Some(
                        input
                            .source_identity_id
                            .as_deref()
                            .ok_or(DbError::VersionConflict)?,
                    )
                || stored_content != input.handoff_content
                || stored_content_guard != input.content_guard_json
                || stored_correlation_id != input.correlation_id
                || stored_causation_id != input.causation_id
            {
                return Err(DbError::VersionConflict);
            }
            let stored_source_value =
                serde_json::from_str::<serde_json::Value>(&stored_source_revisions)
                    .map_err(|_| DbError::VersionConflict)?;
            let stored_source = stored_source_value
                .get("source")
                .ok_or(DbError::VersionConflict)?;
            let stored_project = stored_source_value
                .get("project")
                .ok_or(DbError::VersionConflict)?;
            let stored_target = stored_source_value
                .get("target")
                .ok_or(DbError::VersionConflict)?;
            let stored_request = stored_source_value
                .get("request")
                .ok_or(DbError::VersionConflict)?;
            if json_string(stored_source, "identity_id") != input.source_identity_id.as_deref()
                || json_string(stored_source, "profile_revision_id")
                    != input.source_profile_id.as_deref()
                || json_string(stored_source, "instruction_revision_id")
                    != input.source_instruction_revision_id.as_deref()
                || json_string(stored_source, "message_id") != input.source_message_id.as_deref()
                || json_string(stored_source, "turn_id") != input.source_turn_id.as_deref()
                || json_string(stored_project, "id") != Some(input.project.id.as_str())
                || json_string(stored_project, "name") != Some(input.project.name.as_str())
                || json_string(stored_project, "mode")
                    != Some(approval.approved_project_mode.as_str())
                || json_string(stored_target, "chat_id") != Some(project_chat_id.as_str())
                || json_string(stored_target, "binding_id")
                    != Some(input.project_agent_binding_id.as_str())
                || json_string(stored_target, "message_id")
                    != Some(input.target_message_id.as_str())
                || json_string(stored_target, "turn_id") != Some(input.target_turn_id.as_str())
                || json_string(stored_request, "policy_revision")
                    != Some(input.policy_revision.as_str())
                || json_string(stored_request, "policy_digest")
                    != Some(input.policy_digest.as_str())
            {
                return Err(DbError::VersionConflict);
            }
            let stored_source_digest = stored_source_value
                .pointer("/request/source_revisions_digest")
                .and_then(serde_json::Value::as_str)
                .ok_or(DbError::VersionConflict)?;
            let request_value =
                serde_json::from_str::<serde_json::Value>(&input.source_revisions_json)
                    .map_err(|_| DbError::VersionConflict)?;
            if stored_source_digest != handoff_request_fingerprint(&request_value, &input)? {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(CreatedProjectFromCharterApproval {
                project,
                project_agent_binding_id: binding_id,
                project_chat_id,
                charter_id: approval.charter_id,
                charter_revision_id: approval.revision_id,
                handoff_id,
                target_message_id,
                target_turn_id,
            });
        }
        if approval.lifecycle != "active" {
            return Err(DbError::VersionConflict);
        }
        let genesis_lifecycle: Option<String> = approval_row.try_get("genesis_lifecycle")?;
        if genesis_lifecycle.as_deref() != Some("ready_for_project") {
            return Err(DbError::VersionConflict);
        }
        let revision_content_digest: String = approval_row.try_get("revision_content_digest")?;
        let revision_rendered_digest: String = approval_row.try_get("revision_rendered_digest")?;
        let current_approved_revision_id: Option<String> =
            approval_row.try_get("current_approved_revision_id")?;
        if approval.content_digest != revision_content_digest
            || approval.rendered_digest != revision_rendered_digest
            || current_approved_revision_id.as_deref() != Some(approval.revision_id.as_str())
            || approval.selected_identity_id.is_none()
            || approval.selected_profile_id.is_none()
            || approval.selected_operating_skill_revision_id.is_none()
            || approval.approval_event_id.is_none()
        {
            return Err(DbError::VersionConflict);
        }
        let genesis_session_id: String = approval_row
            .try_get::<Option<String>, _>("genesis_session_id")?
            .ok_or(DbError::VersionConflict)?;
        let author_identity_id = input
            .source_identity_id
            .clone()
            .ok_or(DbError::VersionConflict)?;
        let historical_author: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_identity
             WHERE id = ? AND owner_id = ? LIMIT 1",
        )
        .bind(&author_identity_id)
        .bind(&input.account_id)
        .fetch_optional(&mut *tx)
        .await?;
        if historical_author.is_none() {
            return Err(DbError::VersionConflict);
        }
        if let Some(source_profile_id) = input.source_profile_id.as_deref() {
            let profile_ok: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_profile p
                 JOIN agent_identity i ON i.id = p.identity_id
                 WHERE p.id = ? AND p.identity_id = ? AND i.owner_id = ? LIMIT 1",
            )
            .bind(source_profile_id)
            .bind(&author_identity_id)
            .bind(&input.account_id)
            .fetch_optional(&mut *tx)
            .await?;
            if profile_ok.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        if let Some(source_instruction_revision_id) =
            input.source_instruction_revision_id.as_deref()
        {
            let instruction_ok: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_chat_instruction_revision
                 WHERE id = ? AND chat_id = ? LIMIT 1",
            )
            .bind(source_instruction_revision_id)
            .bind(&main_chat_id)
            .fetch_optional(&mut *tx)
            .await?;
            if instruction_ok.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        if let Some(source_message_id) = input.source_message_id.as_deref() {
            let message_ok: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_chat_message
                 WHERE id = ? AND chat_id = ? LIMIT 1",
            )
            .bind(source_message_id)
            .bind(&main_chat_id)
            .fetch_optional(&mut *tx)
            .await?;
            if message_ok.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        if let Some(source_turn_id) = input.source_turn_id.as_deref() {
            let turn_ok: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_chat_turn_job
                 WHERE id = ? AND chat_id = ? AND responder_identity_id = ?
                   AND (? IS NULL OR profile_id = ?)
                 LIMIT 1",
            )
            .bind(source_turn_id)
            .bind(&main_chat_id)
            .bind(&author_identity_id)
            .bind(input.source_profile_id.as_deref())
            .bind(input.source_profile_id.as_deref())
            .fetch_optional(&mut *tx)
            .await?;
            if turn_ok.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        let identity_id = approval
            .selected_identity_id
            .clone()
            .ok_or(DbError::VersionConflict)?;
        let profile_id = approval
            .selected_profile_id
            .clone()
            .ok_or(DbError::VersionConflict)?;
        let skill_revision_id = approval
            .selected_operating_skill_revision_id
            .clone()
            .ok_or(DbError::VersionConflict)?;
        let project_mode: String = approval_row.try_get("approved_project_mode")?;
        if !matches!(project_mode.as_str(), "compact" | "standard") {
            return Err(DbError::Check(
                "approved Project mode is invalid".to_owned(),
            ));
        }
        if approval.approved_name.as_deref() != Some(input.project.name.as_str())
            || approval.selected_policy_digest.as_deref() != Some(input.policy_digest.as_str())
            || approval.selected_policy_revision.as_deref() != Some(input.policy_revision.as_str())
            || input.project.owner_id.as_deref() != Some(input.account_id.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        let requested_mode = serde_json::from_str::<serde_json::Value>(&input.project.settings)
            .ok()
            .and_then(|settings| {
                settings
                    .get("project_mode")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        if requested_mode.as_deref() != Some(project_mode.as_str()) {
            return Err(DbError::VersionConflict);
        }
        let name_taken: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project
             WHERE owner_id = ? AND name = ? LIMIT 1",
        )
        .bind(&input.account_id)
        .bind(&input.project.name)
        .fetch_optional(&mut *tx)
        .await?;
        if name_taken.is_some() {
            return Err(DbError::VersionConflict);
        }

        let selected = sqlx::query(
            "SELECT p.tool_policy_json, i.paused, i.archived_at,
                    i.selected_profile_id, sr.id AS skill_revision_id,
                    sr.skill_key, sr.policy_digest, sr.content_digest,
                    s.current_revision_id, s.lifecycle
             FROM agent_profile p
             JOIN agent_identity i ON i.id = p.identity_id
             JOIN operating_skill_revision sr ON sr.id = ?
             JOIN operating_skill s ON s.id = sr.operating_skill_id
             WHERE p.id = ? AND p.identity_id = ? AND i.owner_id = ?
               AND i.selected_profile_id = p.id
             LIMIT 1",
        )
        .bind(&skill_revision_id)
        .bind(&profile_id)
        .bind(&identity_id)
        .bind(&input.account_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(selected) = selected else {
            return Err(DbError::VersionConflict);
        };
        let selected_paused: i64 = selected.try_get("paused")?;
        let selected_archived: Option<String> = selected.try_get("archived_at")?;
        let selected_profile_id: Option<String> = selected.try_get("selected_profile_id")?;
        let selected_skill_revision_id: String = selected.try_get("skill_revision_id")?;
        let selected_skill_key: String = selected.try_get("skill_key")?;
        let selected_skill_policy_digest: String = selected.try_get("policy_digest")?;
        let selected_skill_content_digest: String = selected.try_get("content_digest")?;
        let selected_skill_current_revision_id: Option<String> =
            selected.try_get("current_revision_id")?;
        let selected_skill_lifecycle: String = selected.try_get("lifecycle")?;
        let selected_tool_policy_json: String = selected.try_get("tool_policy_json")?;
        if selected_profile_id.as_deref() != Some(profile_id.as_str())
            || selected_skill_revision_id != skill_revision_id
            || selected_skill_current_revision_id.as_deref() != Some(skill_revision_id.as_str())
            || selected_skill_lifecycle != "active"
            || selected_paused != 0
            || selected_archived.is_some()
            || selected_skill_key != PROJECT_OPERATING_SKILL_KEY
            || selected_skill_policy_digest.trim().is_empty()
            || selected_skill_content_digest.trim().is_empty()
            || profile_policy_digest(&selected_tool_policy_json) != input.policy_digest
        {
            return Err(DbError::VersionConflict);
        }

        sqlx::query(
            "INSERT INTO project (
                id, name, settings, workflow_definition, workflow_template_name,
                primary_repo_id, owner_id, project_hooks_json, project_work_epoch,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, NULL, ?, ?, '[]', 0, ?, ?)",
        )
        .bind(&input.project.id)
        .bind(&input.project.name)
        .bind(&input.project.settings)
        .bind(&input.project.workflow_definition)
        .bind(input.project.primary_repo_id.as_deref())
        .bind(input.project.owner_id.as_deref())
        .bind(&input.project.created_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;

        let (project_chat_id, setup_binding_id): (String, String) = sqlx::query_as(
            "SELECT c.id, b.id FROM agent_chat c
             JOIN project_agent_binding b ON b.project_id = c.project_id
               AND b.state = 'agent_setup_required'
             WHERE c.project_id = ? AND c.kind = 'project'",
        )
        .bind(&input.project.id)
        .fetch_one(&mut *tx)
        .await?;

        let replaced_setup = sqlx::query(
            "UPDATE project_agent_binding
             SET state = 'replaced', replacement_reason = 'Charter-backed Project creation',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND state = 'agent_setup_required'",
        )
        .bind(&input.project.updated_at)
        .bind(&setup_binding_id)
        .execute(&mut *tx)
        .await?;
        if replaced_setup.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let active_binding = sqlx::query(
            "INSERT INTO project_agent_binding (
                id, project_id, identity_id, profile_id, state,
                autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                wake_budget, version, replaced_by_binding_id,
                operating_skill_revision_id, policy_revision, policy_digest,
                charter_id, charter_revision_id, charter_setup_required,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'active', '{}', ?, '[]', ?, 1, NULL,
                       ?, ?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(&input.project_agent_binding_id)
        .bind(&input.project.id)
        .bind(&identity_id)
        .bind(&profile_id)
        .bind(PROJECT_AGENT_PERMISSION_CEILING)
        .bind(crate::DEFAULT_PROJECT_AGENT_WAKE_BUDGET)
        .bind(&skill_revision_id)
        .bind(
            approval
                .selected_policy_revision
                .as_deref()
                .ok_or(DbError::VersionConflict)?,
        )
        .bind(&input.policy_digest)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(&input.project.created_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let linked_setup = sqlx::query(
            "UPDATE project_agent_binding SET replaced_by_binding_id = ?
             WHERE id = ? AND state = 'replaced'",
        )
        .bind(&input.project_agent_binding_id)
        .bind(&setup_binding_id)
        .execute(&mut *tx)
        .await?;
        if active_binding.rows_affected() != 1 || linked_setup.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let ready_chat = sqlx::query(
            "UPDATE agent_chat SET status = 'ready', version = version + 1, updated_at = ?
             WHERE id = ? AND status = 'agent_setup_required'",
        )
        .bind(&input.project.updated_at)
        .bind(&project_chat_id)
        .execute(&mut *tx)
        .await?;

        if ready_chat.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        // Attach the already-approved Charter before setting the Project
        // pointer; the migration trigger deliberately requires this order.
        let attached_charter = sqlx::query(
            "UPDATE project_charter SET project_id = ?, lifecycle = 'attached', updated_at = ?
             WHERE id = ? AND project_id IS NULL",
        )
        .bind(&input.project.id)
        .bind(&input.project.updated_at)
        .bind(&approval.charter_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if attached_charter.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let backed_project = sqlx::query(
            "UPDATE project SET charter_status = 'charter_backed', charter_setup_required = 0,
                 current_charter_id = ?, current_charter_revision_id = ?,
                 current_charter_version = ?, version = version + 1, updated_at = ?
             WHERE id = ?",
        )
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(approval.expected_charter_version + 1)
        .bind(&input.project.updated_at)
        .bind(&input.project.id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if backed_project.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        // Compact Projects start with one explicit, planned M001. Acceptance
        // statements already approved in the Charter become manual checks;
        // the Project Agent may refine the immutable definition later. A
        // milestone is not runnable or release-authoritative until a user
        // approves and activates an execution baseline containing it.
        if project_mode == "compact" {
            let milestone_id = new_uuid_v4();
            let milestone_revision_id = new_uuid_v4();
            let revision_content_json: String = approval_row.try_get("revision_content_json")?;
            let revision_content: serde_json::Value = serde_json::from_str(&revision_content_json)
                .map_err(|error| {
                    DbError::Check(format!("approved Charter content is invalid JSON: {error}"))
                })?;
            let acceptance_statements = revision_content
                .get("success")
                .and_then(serde_json::Value::as_object)
                .and_then(|success| success.get("acceptance_statements"))
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    DbError::Check(
                        "approved Charter acceptance_statements must be an array".to_owned(),
                    )
                })?;
            if acceptance_statements.is_empty() {
                return Err(DbError::Check(
                    "approved Charter acceptance_statements must not be empty".to_owned(),
                ));
            }
            let mut acceptance_checks = Vec::new();
            let mut acceptance_check_rows = Vec::new();
            for (index, statement) in acceptance_statements.iter().enumerate() {
                let description = statement
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        DbError::Check(format!(
                            "approved Charter acceptance statement {index} must be a non-empty string"
                        ))
                    })?;
                let check_id = new_uuid_v4();
                acceptance_checks.push(serde_json::json!({
                    "id": check_id.clone(),
                    "description": description,
                    "required": true,
                    "source_kind": "manual",
                    "expected_result": "passed",
                }));
                acceptance_check_rows.push((
                    check_id,
                    format!("acceptance-{}", index + 1),
                    description.to_owned(),
                ));
            }
            let milestone_outcome = format!("Initial outcome for {}", input.project.name);
            let milestone_content_json = serde_json::json!({
                "name": "M1 — Deliver outcome",
                "outcome": milestone_outcome,
                "included_scope": [],
                "excluded_scope": [],
                "charter_revision": approval.revision_id,
                "document_revisions": [],
                "task_ids": [],
                "dependencies": [],
                "risks": [],
                "acceptance_checks": acceptance_checks.clone(),
                "evidence_requirements": [],
                "known_issues": [],
            })
            .to_string();
            let milestone_rendered_view =
                format!("# M1 — Deliver outcome\n\n{}", milestone_outcome);
            let milestone_content_digest = sha256_hex(milestone_content_json.as_bytes());
            let milestone_rendered_digest = sha256_hex(milestone_rendered_view.as_bytes());
            sqlx::query(
                "INSERT INTO project_milestone (
                    id, project_id, milestone_sequence, milestone_key, display_label,
                    lifecycle, blocker_reason_json, stale_reason_json,
                    reconciliation_reason_json, version, created_at, updated_at
                 ) VALUES (?, ?, 1, 'M001', 'M1 — Deliver outcome', 'planned', '[]', '[]', '[]', 1, ?, ?)",
            )
            .bind(&milestone_id)
            .bind(&input.project.id)
            .bind(&input.project.created_at)
            .bind(&input.project.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            sqlx::query(
                "INSERT INTO project_milestone_revision (
                    id, milestone_id, revision, base_revision, lifecycle,
                    display_label, outcome, included_scope_json, excluded_scope_json,
                    charter_revision_id, document_revisions_json, task_selection_json,
                    dependencies_json, risks_json, acceptance_checks_json,
                    evidence_requirements_json, known_issues_json, change_summary,
                    schema_version, render_version, rendered_view, content_digest,
                    rendered_digest,
                    author_type, author_id, source_refs_json, created_at
                 ) VALUES (?, ?, 1, 0, 'approved', 'M1 — Deliver outcome', ?, '[]', '[]', ?, '[]', '[]',
                           '[]', '[]', ?, '[]', '[]', 'Genesis baseline',
                           'forge.project-orchestration/v1', '1', ?, ?, ?, 'system',
                           'forge.project_creation', '[]', ?)",
            )
            .bind(&milestone_revision_id)
            .bind(&milestone_id)
            .bind(&milestone_outcome)
            .bind(&approval.revision_id)
            .bind(serde_json::to_string(&acceptance_checks).map_err(|error| {
                DbError::Check(format!("invalid compact milestone checks: {error}"))
            })?)
            .bind(&milestone_rendered_view)
            .bind(&milestone_content_digest)
            .bind(&milestone_rendered_digest)
            .bind(&input.project.created_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            for (check_id, check_key, description) in acceptance_check_rows {
                sqlx::query(
                    "INSERT INTO project_milestone_check (
                        id, project_id, milestone_id, definition_revision_id,
                        check_key, description, required, source_kind,
                        expected_result, evidence_required, version,
                        current_result_id, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, ?, ?, 1, 'manual', 'passed', 0, 1, NULL, ?, ?)",
                )
                .bind(check_id)
                .bind(&input.project.id)
                .bind(&milestone_id)
                .bind(&milestone_revision_id)
                .bind(check_key)
                .bind(description)
                .bind(&input.project.created_at)
                .bind(&input.project.updated_at)
                .execute(&mut *tx)
                .await
                .map_err(orchestration_write_error)?;
            }
            let milestone_pointer = sqlx::query(
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?, version = version + 1,
                     updated_at = ? WHERE id = ? AND project_id = ?",
            )
            .bind(&milestone_revision_id)
            .bind(&input.project.updated_at)
            .bind(&milestone_id)
            .bind(&input.project.id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if milestone_pointer.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
            let project_pointer = sqlx::query(
                "UPDATE project SET primary_milestone_id = ?, version = version + 1,
                     updated_at = ? WHERE id = ?",
            )
            .bind(&milestone_id)
            .bind(&input.project.updated_at)
            .bind(&input.project.id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if project_pointer.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }

        sqlx::query(
            "INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at)
             VALUES (?, ?, ?, 'owner', ?, ?)",
        )
        .bind(&input.member_id)
        .bind(&input.project.id)
        .bind(&input.account_id)
        .bind(&input.project.created_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;

        let sequence: i64 = sqlx::query_scalar(
            "UPDATE agent_chat SET message_count = message_count + 1,
                    last_message_at = ?, version = version + 1, updated_at = ?
             WHERE id = ? RETURNING message_count - 1",
        )
        .bind(&input.project.updated_at)
        .bind(&input.project.updated_at)
        .bind(&project_chat_id)
        .fetch_one(&mut *tx)
        .await?;
        let source_value = serde_json::from_str::<serde_json::Value>(&input.source_revisions_json)
            .map_err(|_| {
                DbError::Check("handoff source_revisions_json must be valid JSON".to_owned())
            })?;
        if !source_value.is_object() {
            return Err(DbError::Check(
                "handoff source_revisions_json must be a JSON object".to_owned(),
            ));
        }
        let source_revisions_json: String = sqlx::query_scalar(
            "SELECT json_set(
                ?,
                '$.handoff_id', ?,
                '$.target.chat_id', ?,
                '$.target.binding_id', ?,
                '$.target.message_id', ?,
                '$.target.turn_id', ?,
                '$.project.id', ?,
                '$.approval_id', ?,
                '$.source.identity_id', ?,
                '$.source.profile_revision_id', ?,
                '$.source.instruction_revision_id', ?,
                '$.source.message_id', ?,
                '$.source.turn_id', ?,
                '$.request.policy_revision', ?,
                '$.request.policy_digest', ?,
                '$.request.source_revisions_digest', ?,
                '$.request.source_revisions_json', ?,
                '$.request.authorization.principal_type', ?,
                '$.request.authorization.principal_id', ?,
                '$.request.authorization.authorization_basis', ?,
                '$.request.authorization.action', ?,
                '$.request.authorization.event_id', ?,
                '$.request.authorization.occurred_at', ?,
                '$.delivery.delivered_at', ?
             )",
        )
        .bind(&input.source_revisions_json)
        .bind(&input.handoff_id)
        .bind(&project_chat_id)
        .bind(&input.project_agent_binding_id)
        .bind(&input.target_message_id)
        .bind(&input.target_turn_id)
        .bind(&input.project.id)
        .bind(&input.approval_id)
        .bind(&author_identity_id)
        .bind(input.source_profile_id.as_deref())
        .bind(input.source_instruction_revision_id.as_deref())
        .bind(input.source_message_id.as_deref())
        .bind(input.source_turn_id.as_deref())
        .bind(&input.policy_revision)
        .bind(&input.policy_digest)
        .bind(handoff_request_fingerprint(&source_value, &input)?)
        .bind(&input.source_revisions_json)
        .bind(&input.create_principal_type)
        .bind(&input.create_principal_id)
        .bind(&input.create_authorization_basis)
        .bind(&input.create_action)
        .bind(&input.create_event_id)
        .bind(&input.create_occurred_at)
        .bind(&input.project.updated_at)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO agent_handoff (
                id, source_chat_id, target_chat_id, source_message_id,
                source_turn_job_id, target_message_id,
                target_turn_job_id, author_identity_id, content, content_guard_json,
                source_revisions_json, status, correlation_id, causation_id,
                dedupe_key, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'delivered', ?, ?, ?, ?, ?)",
        )
        .bind(&input.handoff_id)
        .bind(&main_chat_id)
        .bind(&project_chat_id)
        .bind(input.source_message_id.as_deref())
        .bind(input.source_turn_id.as_deref())
        .bind(&input.target_message_id)
        .bind(&input.target_turn_id)
        .bind(Some(&author_identity_id))
        .bind(&input.handoff_content)
        .bind(&input.content_guard_json)
        .bind(&source_revisions_json)
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(&input.idempotency_key)
        .bind(&input.project.created_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        sqlx::query(
            "INSERT INTO agent_chat_message (
                id, chat_id, sequence, author_type, author_id, content,
                content_guard_json, sensitivity, status, outcome, profile_id,
                correlation_id,
                causation_id, handoff_id, source_type, source_id,
                source_metadata_json, created_at
             ) VALUES (?, ?, ?, 'handoff', ?, ?, ?, 'internal', 'complete',
                       'handoff_delivered', ?, ?, ?, ?,
                       'handoff', ?, ?, ?)",
        )
        .bind(&input.target_message_id)
        .bind(&project_chat_id)
        .bind(sequence)
        .bind(&author_identity_id)
        .bind(&input.handoff_content)
        .bind(&input.content_guard_json)
        .bind(input.source_profile_id.as_deref())
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(&input.handoff_id)
        .bind(&input.handoff_id)
        .bind(
            serde_json::json!({
                "source_chat_id": main_chat_id.clone(),
                "source_identity_id": author_identity_id.clone(),
                "source_profile_id": input.source_profile_id.clone(),
            })
            .to_string(),
        )
        .bind(&input.project.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        sqlx::query(
            "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, responder_identity_id, profile_id,
                canonical_scope_type, canonical_scope_id, status, dedupe_key,
                max_attempts, correlation_id, causation_id, causation_depth,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'agent_chat', ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.target_turn_id)
        .bind(&project_chat_id)
        .bind(&input.target_message_id)
        .bind(&identity_id)
        .bind(&profile_id)
        .bind(&project_chat_id)
        .bind(format!("handoff:{}", input.idempotency_key))
        .bind(input.max_attempts)
        .bind(&input.correlation_id)
        .bind(Some(input.handoff_id.as_str()))
        .bind(input.causation_depth.saturating_add(1))
        .bind(&input.project.created_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        sqlx::query(
            "INSERT INTO agent_handoff_delivery (
                handoff_id, delivery_sequence, status, target_message_id,
                target_turn_job_id, created_at
             ) VALUES (?, 1, 'delivered', ?, ?, ?)",
        )
        .bind(&input.handoff_id)
        .bind(&input.target_message_id)
        .bind(&input.target_turn_id)
        .bind(&input.project.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;

        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.created_from_charter_approval".to_owned(),
            entity_type: "project".to_owned(),
            entity_id: input.project.id.clone(),
            actor_type: input.create_principal_type.clone(),
            actor_id: Some(input.create_principal_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: input.project.id.clone(),
            correlation_id: input.correlation_id.clone(),
            causation_id: input.causation_id.clone(),
            causation_depth: input.causation_depth,
            dedupe_key: Some(format!("project-charter-create:{}", input.idempotency_key)),
            payload_json: serde_json::json!({
                "project_id": input.project.id,
                "charter_id": approval.charter_id,
                "charter_revision_id": approval.revision_id,
                "approval_id": approval.id,
                "handoff_id": input.handoff_id,
                "project_chat_id": project_chat_id,
                "project_agent_binding_id": input.project_agent_binding_id,
                "authorization": {
                    "principal_type": input.create_principal_type,
                    "principal_id": input.create_principal_id,
                    "authorization_basis": input.create_authorization_basis,
                    "action": input.create_action,
                    "event_id": input.create_event_id,
                    "occurred_at": input.create_occurred_at,
                },
            })
            .to_string(),
            created_at: input.project.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;

        let handed_off_genesis = sqlx::query(
            "UPDATE product_genesis_session
             SET project_id = ?, handoff_id = ?, charter_id = ?, charter_revision_id = ?,
                 charter_approval_id = ?, charter_version = ?, lifecycle = 'handed_off',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'ready_for_project'",
        )
        .bind(&input.project.id)
        .bind(&input.handoff_id)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(&approval.id)
        .bind(approval.expected_charter_version + 1)
        .bind(&input.project.updated_at)
        .bind(&genesis_session_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if handed_off_genesis.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let consumed_approval = sqlx::query(
            "UPDATE project_charter_approval
             SET lifecycle = 'consumed', consumed_project_id = ?, consumed_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(&input.project.id)
        .bind(&input.project.updated_at)
        .bind(&input.project.updated_at)
        .bind(&approval.id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if consumed_approval.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, reason,
                idempotency_key, occurred_at, created_at
             ) VALUES (?, ?, 'consumed', ?, ?, ?, ?, ?, 'Project created', ?, ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&approval.id)
        .bind(&input.create_principal_type)
        .bind(&input.create_principal_id)
        .bind(&input.create_authorization_basis)
        .bind(&input.create_action)
        .bind(&input.create_event_id)
        .bind(format!("{}:consumed", input.idempotency_key))
        .bind(&input.create_occurred_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;

        let project_row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(&input.project.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(DbError::from)?;
        let project = map_project(project_row)?;
        tx.commit().await?;
        Ok(CreatedProjectFromCharterApproval {
            project,
            project_agent_binding_id: input.project_agent_binding_id,
            project_chat_id,
            charter_id: approval.charter_id,
            charter_revision_id: approval.revision_id,
            handoff_id: input.handoff_id,
            target_message_id: input.target_message_id,
            target_turn_id: input.target_turn_id,
        })
    }

    async fn create_project_canonical_conflict(
        &self,
        input: CreateProjectCanonicalConflict,
    ) -> Result<ProjectCanonicalConflictRecord> {
        if input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) =
            sqlx::query("SELECT * FROM project_canonical_conflict WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut *tx)
                .await?
                .map(map_canonical_conflict)
                .transpose()?
        {
            let same = existing.project_id == input.project_id
                && existing.domain == input.domain
                && existing.governing_record_type == input.governing_record_type
                && existing.governing_record_id == input.governing_record_id
                && existing.governing_record_revision == input.governing_record_revision
                && existing.governing_record_digest == input.governing_record_digest
                && existing.conflicting_record_type == input.conflicting_record_type
                && existing.conflicting_record_id == input.conflicting_record_id
                && existing.conflicting_record_revision == input.conflicting_record_revision
                && existing.conflicting_record_digest == input.conflicting_record_digest
                && existing.affected_paths_json == input.affected_paths_json
                && existing.conflict_code == input.conflict_code
                && existing.description == input.description
                && existing.detected_by_type == input.detected_by_type
                && existing.detected_by_id == input.detected_by_id
                && existing.authorization_basis == input.authorization_basis
                && existing.authorization_action == input.authorization_action
                && existing.explicit_event == input.explicit_event
                && existing.authorization_occurred_at == input.authorization_occurred_at;
            if !same {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let row = sqlx::query(
            "INSERT INTO project_canonical_conflict (
                id, project_id, domain, governing_record_type,
                governing_record_id, governing_record_revision,
                governing_record_digest, conflicting_record_type,
                conflicting_record_id, conflicting_record_revision,
                conflicting_record_digest, affected_paths_json, conflict_code,
                description, detected_by_type, detected_by_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING *",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.domain)
        .bind(&input.governing_record_type)
        .bind(&input.governing_record_id)
        .bind(&input.governing_record_revision)
        .bind(&input.governing_record_digest)
        .bind(&input.conflicting_record_type)
        .bind(&input.conflicting_record_id)
        .bind(&input.conflicting_record_revision)
        .bind(&input.conflicting_record_digest)
        .bind(&input.affected_paths_json)
        .bind(&input.conflict_code)
        .bind(&input.description)
        .bind(&input.detected_by_type)
        .bind(input.detected_by_id.as_deref())
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.explicit_event)
        .bind(&input.authorization_occurred_at)
        .bind(&input.idempotency_key)
        .bind(&input.created_at)
        .fetch_one(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let record = map_canonical_conflict(row)?;
        tx.commit().await?;
        Ok(record)
    }

    async fn get_project_canonical_conflict(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCanonicalConflictRecord>> {
        select_one(
            "SELECT * FROM project_canonical_conflict WHERE id = ?",
            self.pool(),
            id,
            map_canonical_conflict,
        )
        .await
    }

    async fn list_project_canonical_conflicts(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectCanonicalConflictRecord>> {
        sqlx::query(
            "SELECT * FROM project_canonical_conflict
             WHERE project_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_canonical_conflict)
        .collect()
    }

    async fn create_project_reconciliation(
        &self,
        input: CreateProjectReconciliation,
    ) -> Result<ProjectReconciliationRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let row = sqlx::query(
            "INSERT INTO project_reconciliation_record (
                id, project_id, conflict_id, record_type, record_id,
                record_revision, record_digest, governing_record_type,
                governing_record_id, governing_record_revision,
                governing_record_digest, state, current_resolution_id, version,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'required', NULL, 1, ?, ?)
             RETURNING *",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.conflict_id)
        .bind(&input.record_type)
        .bind(&input.record_id)
        .bind(&input.record_revision)
        .bind(&input.record_digest)
        .bind(&input.governing_record_type)
        .bind(&input.governing_record_id)
        .bind(&input.governing_record_revision)
        .bind(&input.governing_record_digest)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .fetch_one(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let record = map_reconciliation(row)?;
        tx.commit().await?;
        Ok(record)
    }

    async fn get_project_reconciliation(
        &self,
        id: &str,
    ) -> Result<Option<ProjectReconciliationRecord>> {
        select_one(
            "SELECT * FROM project_reconciliation_record WHERE id = ?",
            self.pool(),
            id,
            map_reconciliation,
        )
        .await
    }

    async fn list_project_reconciliations(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectReconciliationRecord>> {
        sqlx::query(
            "SELECT * FROM project_reconciliation_record
             WHERE project_id = ? ORDER BY updated_at DESC, id DESC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_reconciliation)
        .collect()
    }

    async fn resolve_project_reconciliation(
        &self,
        input: ResolveProjectReconciliation,
    ) -> Result<ProjectReconciliationRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if !matches!(
            input.action.as_str(),
            "retained" | "revised" | "cancelled" | "superseded" | "invalidated"
        ) || input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || input.authorization_occurred_at.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
            || input.reason.trim().is_empty()
            || input.occurred_at.trim().is_empty()
            || !valid_authorization_timestamp(&input.occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        if let Some(existing) = sqlx::query(
            "SELECT r.* FROM project_reconciliation_record r
             JOIN project_reconciliation_resolution resolution
               ON resolution.id = r.current_resolution_id
             WHERE resolution.idempotency_key = ?",
        )
        .bind(&input.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        .map(map_reconciliation)
        .transpose()?
        {
            let resolution = sqlx::query(
                "SELECT action, principal_type, principal_id,
                        authorization_basis, authorization_action, explicit_event,
                        authorization_occurred_at, reason, occurred_at
                 FROM project_reconciliation_resolution
                 WHERE idempotency_key = ?",
            )
            .bind(&input.idempotency_key)
            .fetch_one(&mut *tx)
            .await?;
            let same = existing.id == input.id
                && existing.state == input.action
                && resolution.try_get::<String, _>("action")? == input.action
                && resolution.try_get::<String, _>("principal_type")? == input.principal_type
                && resolution.try_get::<String, _>("principal_id")? == input.principal_id
                && resolution.try_get::<String, _>("authorization_basis")?
                    == input.authorization_basis
                && resolution.try_get::<String, _>("authorization_action")?
                    == input.authorization_action
                && resolution.try_get::<String, _>("explicit_event")? == input.explicit_event
                && resolution.try_get::<String, _>("authorization_occurred_at")?
                    == input.authorization_occurred_at
                && resolution.try_get::<String, _>("reason")? == input.reason
                && resolution.try_get::<String, _>("occurred_at")? == input.occurred_at;
            if !same {
                return Err(DbError::VersionConflict);
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let current = sqlx::query(
            "SELECT * FROM project_reconciliation_record
             WHERE id = ? AND state = 'required'",
        )
        .bind(&input.id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let current_version: i64 = current.try_get("version")?;
        if current_version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_reconciliation_resolution (
                id, reconciliation_id, action, principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, reason, occurred_at, idempotency_key,
                created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.resolution_id)
        .bind(&input.id)
        .bind(&input.action)
        .bind(&input.principal_type)
        .bind(&input.principal_id)
        .bind(&input.authorization_basis)
        .bind(&input.authorization_action)
        .bind(&input.explicit_event)
        .bind(&input.authorization_occurred_at)
        .bind(&input.reason)
        .bind(&input.occurred_at)
        .bind(&input.idempotency_key)
        .bind(&input.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let updated = sqlx::query(
            "UPDATE project_reconciliation_record
             SET state = ?, current_resolution_id = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND state = 'required' AND version = ?",
        )
        .bind(&input.action)
        .bind(&input.resolution_id)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let row = sqlx::query("SELECT * FROM project_reconciliation_record WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        let record = map_reconciliation(row)?;
        tx.commit().await?;
        Ok(record)
    }
}
