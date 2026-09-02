use super::command_finalization::{action_scope_resolves_to_command_scope, finalize_command_in_tx};
use super::*;
use crate::*;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteRow, Row};
use std::collections::BTreeSet;

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

pub(super) fn orchestration_write_error(error: sqlx::Error) -> DbError {
    if let sqlx::Error::Database(database_error) = &error {
        let message = database_error.message().to_ascii_lowercase();
        if message.contains("unique constraint") || message.contains("constraint failed") {
            return DbError::VersionConflict;
        }
    }
    check_error(error)
}

async fn materialize_milestone_check_definitions_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    project_id: &str,
    milestone_id: &str,
    revision: &CreateProjectMilestoneRevision,
    checks: &[CreateProjectMilestoneCheck],
) -> Result<()> {
    if revision.lifecycle == "draft" {
        if checks.is_empty() {
            return Ok(());
        }
        return Err(DbError::VersionConflict);
    }

    let declared: Vec<serde_json::Value> = serde_json::from_str(&revision.acceptance_checks_json)
        .map_err(|error| {
        DbError::Check(format!(
            "milestone acceptance-check definition JSON is invalid: {error}"
        ))
    })?;
    let declared_ids = declared
        .iter()
        .map(|value| {
            value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    DbError::Check(
                        "milestone acceptance checks require stable non-empty ids".to_owned(),
                    )
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let supplied_ids = checks
        .iter()
        .map(|check| check.id.clone())
        .collect::<BTreeSet<_>>();
    if declared_ids.len() != declared.len()
        || supplied_ids.len() != checks.len()
        || supplied_ids != declared_ids
    {
        return Err(DbError::VersionConflict);
    }

    let declared_evidence: Vec<serde_json::Value> =
        serde_json::from_str(&revision.evidence_requirements_json).map_err(|error| {
            DbError::Check(format!(
                "milestone evidence-requirement definition JSON is invalid: {error}"
            ))
        })?;
    let mut evidence_ids = BTreeSet::new();
    let mut required_evidence_ids = BTreeSet::new();
    for value in &declared_evidence {
        let id = value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                DbError::Check(
                    "milestone evidence requirements require stable non-empty ids".to_owned(),
                )
            })?;
        if !evidence_ids.insert(id.to_owned()) {
            return Err(DbError::VersionConflict);
        }
        if value
            .get("required")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            required_evidence_ids.insert(id.to_owned());
        }
    }

    for check in checks {
        if check.id.trim().is_empty()
            || check.project_id != project_id
            || check.milestone_id != milestone_id
            || check.definition_revision_id != revision.id
            || check.expected_milestone_version != revision.expected_milestone_version
            || check.check_key != check.id
            || check.description.trim().is_empty()
            // `task_validation` joins the definable kinds now that
            // `project.validation` gives it a receipt-backed result path.
            || !matches!(
                check.source_kind.as_str(),
                "manual" | "policy_waiver" | "task_validation"
            )
            || check.evidence_required != required_evidence_ids.contains(&check.id)
            || check.updated_at != revision.created_at
            || check.created_at.trim().is_empty()
        {
            return Err(DbError::VersionConflict);
        }
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT project_id, milestone_id, definition_revision_id
             FROM project_milestone_check WHERE id = ?",
        )
        .bind(&check.id)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some((existing_project, existing_milestone, existing_revision)) = existing {
            if existing_project != project_id || existing_milestone != milestone_id {
                return Err(DbError::VersionConflict);
            }
            if existing_revision == revision.id {
                return Err(DbError::VersionConflict);
            }
            let updated = sqlx::query(
                "UPDATE project_milestone_check
                 SET definition_revision_id = ?, check_key = ?, description = ?, required = ?,
                     source_kind = ?, expected_result = ?, evidence_required = ?,
                     version = version + 1, current_result_id = NULL, updated_at = ?
                 WHERE id = ? AND project_id = ? AND milestone_id = ?",
            )
            .bind(&revision.id)
            .bind(&check.check_key)
            .bind(&check.description)
            .bind(check.required)
            .bind(&check.source_kind)
            .bind(&check.expected_result)
            .bind(check.evidence_required)
            .bind(&check.updated_at)
            .bind(&check.id)
            .bind(project_id)
            .bind(milestone_id)
            .execute(&mut **tx)
            .await
            .map_err(orchestration_write_error)?;
            if updated.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        } else {
            sqlx::query(
                "INSERT INTO project_milestone_check (
                    id, project_id, milestone_id, definition_revision_id, check_key,
                    description, required, source_kind, expected_result,
                    evidence_required, version, current_result_id, created_at, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)",
            )
            .bind(&check.id)
            .bind(project_id)
            .bind(milestone_id)
            .bind(&revision.id)
            .bind(&check.check_key)
            .bind(&check.description)
            .bind(check.required)
            .bind(&check.source_kind)
            .bind(&check.expected_result)
            .bind(check.evidence_required)
            .bind(&check.created_at)
            .bind(&check.updated_at)
            .execute(&mut **tx)
            .await
            .map_err(orchestration_write_error)?;
        }
    }
    Ok(())
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
/// transaction fills in from durable rows.  Project, binding, handoff,
/// message, turn, chat, correlation, and creation-time values are transport
/// allocations; Charter, approval, policy, authorization, and semantic
/// content remain part of the canonical packet fingerprint.
fn normalize_handoff_request(value: &serde_json::Value) -> Result<serde_json::Value> {
    normalize_project_handoff_request(value).map_err(DbError::Check)
}

fn handoff_request_fingerprint(
    value: &serde_json::Value,
    input: &CreateProjectFromCharterApproval,
) -> Result<String> {
    // The create authorization is part of the immutable request identity, but
    // it is supplied as a typed input rather than trusted from handoff prose.
    // Include it in the normalized digest so a replay under a different
    // principal, action, event, or timestamp cannot reuse the receipt.
    let authorization = serde_json::json!({
        "principal_type": input.create_principal_type,
        "principal_id": input.create_principal_id,
        "authorization_basis": input.create_authorization_basis,
        "action": input.create_action,
        "event_id": input.create_event_id,
        "occurred_at": input.create_occurred_at,
    });
    project_handoff_request_fingerprint(value, &input.source_revisions_json, &authorization)
        .map_err(DbError::Check)
}

fn valid_authorization_timestamp(value: &str) -> bool {
    // The timestamp is immutable user-provided evidence.  Validate its
    // representation here, but do not compare it with this machine's clock:
    // historical imports, delayed/replayed requests, and clock skew must not
    // make an otherwise exact receipt unreadable or mutable.
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

/// Resolve a command receipt before any domain row is read for mutation.  The
/// receipt repository compares principal and digest as part of the lookup, so
/// a changed replay is rejected before a stale version or lifecycle check can
/// produce a different result.
pub(super) async fn resolve_command_replay(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    receipt: Option<&CreateCommandReceipt>,
) -> Result<Option<CommandReceipt>> {
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    if receipt.principal_type.trim().is_empty()
        || receipt.principal_id.trim().is_empty()
        || receipt.scope_type.trim().is_empty()
        || receipt.scope_id.trim().is_empty()
        || receipt.operation.trim().is_empty()
        || receipt.idempotency_key.trim().is_empty()
        || receipt.input_digest.trim().is_empty()
    {
        return Err(DbError::Check(
            "command receipt identity is incomplete".to_owned(),
        ));
    }
    let existing = CommandReceiptRepo::get_command_receipt_in_tx(
        db,
        transaction,
        &receipt.principal_type,
        &receipt.principal_id,
        &receipt.scope_type,
        &receipt.scope_id,
        &receipt.operation,
        &receipt.idempotency_key,
        &receipt.input_digest,
    )
    .await?;

    // A direct Project command has no AgentAction execution to carry its
    // admission facts into the repository layer.  Reauthorize it here, after
    // the receipt lookup and while the caller-owned BEGIN IMMEDIATE writer
    // transaction is still held, before any domain row is read for mutation.
    // Existing receipts intentionally bypass this check: an exact retry must
    // return its frozen outcome even after the binding is paused or replaced.
    if existing.is_none() {
        reauthorize_direct_project_command_in_tx(transaction, receipt).await?;
    }
    Ok(existing)
}

/// Recheck the durable authorization boundary for a fresh direct Project
/// command.  The native adapter performs a friendly preflight, but that read
/// is not an authority boundary: a binding can be revoked between the
/// preflight and the command repository's writer transaction.  This helper is
/// called only after `resolve_command_replay` has acquired `BEGIN IMMEDIATE`,
/// so a concurrent revocation either wins before this check or waits until
/// this command commits.
pub(crate) async fn reauthorize_direct_project_command_in_tx(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    receipt: &CreateCommandReceipt,
) -> Result<()> {
    // User/REST commands and action-backed executions have their own
    // authorization provenance.  Only the receipt-only native Project path
    // needs this binding check here.
    if receipt.agent_action_execution_id.is_some()
        || receipt.principal_type != "agent"
        || receipt.policy_result != "allowed"
    {
        return Ok(());
    }

    if receipt.scope_type != "project" || !is_direct_project_command_operation(&receipt.operation) {
        return Ok(());
    }

    // Adaptive Task commands are direct Project-scoped mutations, but their
    // permission is the Task proposal capability rather than the broader
    // Project-orchestration capability used by the other operations here.
    let required_permission = direct_project_command_permission(&receipt.operation)
        .expect("direct operation must have a permission");

    let identity = sqlx::query(
        "SELECT paused, archived_at, account_permission_ceiling, selected_profile_id
         FROM agent_identity WHERE id = ? LIMIT 1",
    )
    .bind(&receipt.principal_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let paused: i64 = identity.try_get("paused")?;
    let archived_at: Option<String> = identity.try_get("archived_at")?;
    if paused != 0 || archived_at.is_some() {
        return Err(DbError::Check(
            "direct Project command principal is paused or archived".to_owned(),
        ));
    }
    let account_permission_ceiling: String = identity.try_get("account_permission_ceiling")?;
    if !permission_ceiling_contains(&account_permission_ceiling, required_permission) {
        return Err(DbError::Check(
            "direct Project command permission is outside the account ceiling".to_owned(),
        ));
    }
    let selected_profile_id: Option<String> = identity.try_get("selected_profile_id")?;
    let Some(selected_profile_id) = selected_profile_id else {
        return Err(DbError::Check(
            "direct Project command has no selected active profile".to_owned(),
        ));
    };
    let profile_policy: Option<String> = sqlx::query_scalar(
        "SELECT tool_policy_json FROM agent_profile
         WHERE id = ? AND identity_id = ? LIMIT 1",
    )
    .bind(selected_profile_id)
    .bind(&receipt.principal_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if !profile_policy
        .as_deref()
        .is_some_and(|value| permission_ceiling_contains(value, required_permission))
    {
        return Err(DbError::Check(
            "direct Project command permission is outside the selected profile ceiling".to_owned(),
        ));
    }

    let binding = sqlx::query(
        "SELECT permission_ceiling_json
         FROM project_agent_binding
         WHERE project_id = ? AND identity_id = ? AND state = 'active'
         LIMIT 1",
    )
    .bind(&receipt.scope_id)
    .bind(&receipt.principal_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        DbError::Check("direct Project command binding is no longer active".to_owned())
    })?;
    let permission_ceiling: String = binding.try_get("permission_ceiling_json")?;
    if !permission_ceiling_contains(&permission_ceiling, required_permission) {
        return Err(DbError::Check(
            "direct Project command permission is outside the active binding ceiling".to_owned(),
        ));
    }

    let project = sqlx::query(
        "SELECT charter_status, charter_setup_required,
                current_charter_id, current_charter_revision_id
         FROM project WHERE id = ? LIMIT 1",
    )
    .bind(&receipt.scope_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let charter_status: String = project.try_get("charter_status")?;
    let charter_setup_required: i64 = project.try_get("charter_setup_required")?;
    let has_charter = project
        .try_get::<Option<String>, _>("current_charter_id")?
        .is_some()
        && project
            .try_get::<Option<String>, _>("current_charter_revision_id")?
            .is_some();
    let charter_adoption = receipt.operation == "project.charter.adoption";
    let admitted = if charter_adoption {
        (charter_status == "legacy_unverified" && charter_setup_required != 0)
            || (charter_status == "charter_backed" && charter_setup_required == 0 && has_charter)
    } else {
        charter_status == "charter_backed" && charter_setup_required == 0 && has_charter
    };
    if !admitted {
        return Err(DbError::Check(
            "direct Project command is blocked by current Charter state".to_owned(),
        ));
    }
    Ok(())
}

fn permission_ceiling_contains(value: &str, permission: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| match value {
            serde_json::Value::Array(values) => Some(values),
            serde_json::Value::Object(map) => map
                .get("permissions")
                .or_else(|| map.get("allowed"))
                .and_then(serde_json::Value::as_array)
                .cloned(),
            _ => None,
        })
        .is_some_and(|values| {
            values
                .iter()
                .any(|value| value.as_str() == Some(permission))
        })
}

fn is_direct_project_command_operation(operation: &str) -> bool {
    direct_project_command_permission(operation).is_some()
}

fn direct_project_command_permission(operation: &str) -> Option<&'static str> {
    Some(match operation {
        "task.adaptive" => "propose_task",
        "project.charter.adoption"
        | "project.document"
        | "project.decision"
        | "project.execution_baseline.save_draft"
        | "project.execution_baseline.propose_for_approval"
        | "project.milestone"
        | "project.evidence"
        | "project.readiness" => "propose_project",
        _ => return None,
    })
}

pub(super) fn validate_command_scope(
    receipt: Option<&CreateCommandReceipt>,
    scope_type: &str,
    scope_id: &str,
) -> Result<()> {
    if let Some(receipt) = receipt {
        if receipt.scope_type != scope_type || receipt.scope_id != scope_id {
            return Err(DbError::IdempotencyConflict);
        }
    }
    Ok(())
}

pub(super) async fn validate_replay_action_bundle(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    receipt: &CommandReceipt,
    action_execution: Option<&CreateAgentActionExecution>,
) -> Result<()> {
    let Some(expected_id) = receipt.agent_action_execution_id.as_deref() else {
        return if action_execution.is_none() {
            Ok(())
        } else {
            Err(DbError::IdempotencyConflict)
        };
    };

    // The receipt's execution id is authoritative.  A retry may carry a new
    // server-minted execution id, so never use the retry's id to select this
    // row.  The join also proves the original action still has the operation,
    // scope, policy, and causation that the receipt committed.
    let row = sqlx::query(
        "SELECT e.id, e.action_id, e.attempt, e.status, e.result_json, e.error,
                e.executed_by_type, e.executed_by_id, e.idempotency_key,
                a.operation, a.scope_type, a.scope_id, a.policy_result,
                a.correlation_id, a.causation_id, a.causation_depth,
                a.status AS action_status, a.outcome_json AS action_outcome_json
         FROM agent_action_execution e
         JOIN agent_action a ON a.id = e.action_id
         WHERE e.id = ?",
    )
    .bind(expected_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::IdempotencyConflict)?;
    let action_id: String = row.try_get("action_id")?;
    let action_attempt: i64 = row.try_get("attempt")?;
    let action_status: String = row.try_get("status")?;
    let action_result: Option<String> = row.try_get("result_json")?;
    let action_error: Option<String> = row.try_get("error")?;
    let executed_by_type: String = row.try_get("executed_by_type")?;
    let executed_by_id: String = row.try_get("executed_by_id")?;
    let execution_key: String = row.try_get("idempotency_key")?;
    let operation: String = row.try_get("operation")?;
    let action_scope_type: String = row.try_get("scope_type")?;
    let action_scope_id: String = row.try_get("scope_id")?;
    let policy_result: String = row.try_get("policy_result")?;
    let correlation_id: String = row.try_get("correlation_id")?;
    let causation_id: Option<String> = row.try_get("causation_id")?;
    let causation_depth: i64 = row.try_get("causation_depth")?;
    let persisted_action_status: String = row.try_get("action_status")?;
    let persisted_action_outcome: Option<String> = row.try_get("action_outcome_json")?;
    let scope_matches = action_scope_resolves_to_command_scope(
        transaction,
        &action_scope_type,
        &action_scope_id,
        &receipt.scope_type,
        &receipt.scope_id,
    )
    .await?;
    if operation != receipt.operation
        || !scope_matches
        || policy_result != receipt.policy_result
        || correlation_id != receipt.correlation_id
        || causation_id != receipt.causation_id
        || causation_depth != receipt.causation_depth
        || action_result.as_deref() != Some(receipt.outcome_json.as_str())
        || persisted_action_outcome.as_deref() != Some(receipt.outcome_json.as_str())
        || executed_by_type != receipt.principal_type
        || executed_by_id != receipt.principal_id
        || execution_key != receipt.idempotency_key
    {
        return Err(DbError::IdempotencyConflict);
    }

    if let Some(action) = action_execution {
        // `id`, expected version, and timestamps are transport/coordination
        // metadata.  Every persisted execution/provenance field remains
        // bound to the authoritative execution above.
        if action.action_id != action_id
            || action.attempt != action_attempt
            || action.status.to_string() != action_status
            || action.result_json != action_result
            || action.error != action_error
            || action.executed_by_type != executed_by_type
            || action.executed_by_id != executed_by_id
            || action.idempotency_key != execution_key
            || action.action_status.to_string() != persisted_action_status
            || action.action_outcome_json != persisted_action_outcome
        {
            return Err(DbError::IdempotencyConflict);
        }
    }
    Ok(())
}

/// Outcome fields are normally populated by the command service before the
/// repository call.  On replay, compare any typed identity fields present in
/// the frozen JSON with the caller's server-minted ids so a caller cannot
/// present a receipt for one object while asking the repository for another.
pub(super) fn validate_command_outcome_identity(
    receipt: &CommandReceipt,
    identities: &[(&str, &str)],
) -> Result<()> {
    let outcome: serde_json::Value =
        serde_json::from_str(&receipt.outcome_json).map_err(|_| DbError::IdempotencyConflict)?;
    for (key, expected) in identities {
        if let Some(actual) = outcome.get(*key).and_then(serde_json::Value::as_str) {
            if actual != *expected {
                return Err(DbError::IdempotencyConflict);
            }
        }
    }
    Ok(())
}

/// Resolve a server-minted identity from the immutable command outcome.
///
/// Command services may allocate transport/domain primary keys before the
/// repository transaction starts.  Two exact concurrent submissions can
/// therefore carry different candidate ids even though their canonical input
/// digest (which deliberately excludes those ids) is identical.  Once a
/// receipt exists, its outcome is the authority for those ids; replay code
/// must never use the second transport's candidate to find the row.
pub(super) fn command_outcome_string(receipt: &CommandReceipt, key: &str) -> Result<String> {
    let outcome: serde_json::Value =
        serde_json::from_str(&receipt.outcome_json).map_err(|_| DbError::IdempotencyConflict)?;
    outcome
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or(DbError::IdempotencyConflict)
}

fn command_outcome_string_any(receipt: &CommandReceipt, keys: &[&str]) -> Result<String> {
    let outcome: serde_json::Value =
        serde_json::from_str(&receipt.outcome_json).map_err(|_| DbError::IdempotencyConflict)?;
    keys.iter()
        .find_map(|key| {
            outcome
                .get(*key)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
        })
        .map(str::to_owned)
        .ok_or(DbError::IdempotencyConflict)
}

pub(super) fn command_outcome_optional_string(
    receipt: &CommandReceipt,
    key: &str,
) -> Result<Option<String>> {
    let outcome: serde_json::Value =
        serde_json::from_str(&receipt.outcome_json).map_err(|_| DbError::IdempotencyConflict)?;
    match outcome.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Some(value.to_owned()))
            .ok_or(DbError::IdempotencyConflict),
    }
}

fn command_outcome_task(receipt: &CommandReceipt) -> Result<Task> {
    let outcome: serde_json::Value =
        serde_json::from_str(&receipt.outcome_json).map_err(|_| DbError::IdempotencyConflict)?;
    serde_json::from_value(
        outcome
            .get("task")
            .cloned()
            .ok_or(DbError::IdempotencyConflict)?,
    )
    .map_err(|_| DbError::IdempotencyConflict)
}

pub(super) fn command_event_provenance(
    receipt: Option<&CreateCommandReceipt>,
    default_actor_type: String,
    default_actor_id: Option<String>,
    default_correlation_id: String,
    default_causation_id: Option<String>,
    default_causation_depth: i64,
) -> (String, Option<String>, String, Option<String>, i64) {
    receipt.map_or(
        (
            default_actor_type,
            default_actor_id,
            default_correlation_id,
            default_causation_id,
            default_causation_depth,
        ),
        |receipt| {
            (
                receipt.principal_type.clone(),
                Some(receipt.principal_id.clone()),
                receipt.correlation_id.clone(),
                receipt.causation_id.clone(),
                receipt.causation_depth,
            )
        },
    )
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

fn map_reconciliation_resolution(row: SqliteRow) -> Result<ProjectReconciliationResolutionRecord> {
    Ok(ProjectReconciliationResolutionRecord {
        id: required_string(&row, "id")?,
        reconciliation_id: required_string(&row, "reconciliation_id")?,
        action: required_string(&row, "action")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        reason: required_string(&row, "reason")?,
        occurred_at: required_string(&row, "occurred_at")?,
        replacement_ref_type: optional_string(&row, "replacement_ref_type")?,
        replacement_ref_id: optional_string(&row, "replacement_ref_id")?,
        replacement_ref_revision: optional_string(&row, "replacement_ref_revision")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
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
        source_refs_json: required_string(&row, "source_refs_json")?,
        affected_records_json: required_string(&row, "affected_records_json")?,
        supersedes_decision_id: optional_string(&row, "supersedes_decision_id")?,
        created_at: required_string(&row, "created_at")?,
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
        releasing_principal_display_name: optional_string(
            &row,
            "releasing_principal_display_name",
        )?,
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

/// Recheck every mutable source-action and executor authorization fact while
/// the proposal transaction owns SQLite's writer lock.  Service preflight is
/// useful for a friendly error, but it cannot close a race with action,
/// binding, or Project membership changes between the read and the write.
async fn recheck_task_proposal_authorization_in_tx(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    input: &CreateTaskProposalCommand,
    receipt: &CreateCommandReceipt,
) -> Result<()> {
    let (actor_identity_id, scope_type, scope_id, operation, requested_permission, policy_result) =
        if let Some(source_action_id) = input.source_action_id.as_deref() {
            let action = sqlx::query(
                "SELECT actor_identity_id, scope_type, scope_id, operation,
                        requested_permission, policy_result, status, version,
                        target_type, target_id, payload_hash
                 FROM agent_action WHERE id = ? LIMIT 1",
            )
            .bind(source_action_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let actor_identity_id: String = action.try_get("actor_identity_id")?;
            let scope_type: String = action.try_get("scope_type")?;
            let scope_id: String = action.try_get("scope_id")?;
            let operation: String = action.try_get("operation")?;
            let requested_permission: String = action.try_get("requested_permission")?;
            let policy_result: String = action.try_get("policy_result")?;
            let status: String = action.try_get("status")?;
            let version: i64 = action.try_get("version")?;
            let target_type: Option<String> = action.try_get("target_type")?;
            let target_id: Option<String> = action.try_get("target_id")?;
            let payload_hash: String = action.try_get("payload_hash")?;
            if input.expected_action_version != Some(version) {
                return Err(DbError::VersionConflict);
            }
            if actor_identity_id != input.source_actor_identity_id
                || scope_type != input.source_scope_type
                || scope_id != input.source_scope_id
                || operation != input.source_operation
                || requested_permission != input.source_requested_permission
                || policy_result != input.source_policy_result
                || target_type != input.source_target_type
                || target_id != input.source_target_id
                || payload_hash != input.source_payload_hash
                || !matches!(status.as_str(), "proposed" | "approved")
                || policy_result == "denied"
                || (policy_result == "approval_required" && status != "approved")
            {
                return Err(DbError::IdempotencyConflict);
            }
            (
                actor_identity_id,
                scope_type,
                scope_id,
                operation,
                requested_permission,
                policy_result,
            )
        } else {
            if input.expected_action_version.is_some() || input.action_execution.is_some() {
                return Err(DbError::IdempotencyConflict);
            }
            if input.source_operation != "task.propose"
                || input.source_requested_permission != "propose_task"
                || input.source_policy_result != "allowed"
                || input.source_target_type.as_deref() != Some("project")
                || input.source_target_id.as_deref() != Some(input.task.project_id.as_str())
                || receipt.policy_result != input.source_policy_result
            {
                return Err(DbError::IdempotencyConflict);
            }
            (
                input.source_actor_identity_id.clone(),
                input.source_scope_type.clone(),
                input.source_scope_id.clone(),
                input.source_operation.clone(),
                input.source_requested_permission.clone(),
                input.source_policy_result.clone(),
            )
        };

    if operation != "task.propose"
        || requested_permission != "propose_task"
        || policy_result != input.source_policy_result
        || receipt.policy_result != policy_result
    {
        return Err(DbError::IdempotencyConflict);
    }
    if !action_scope_resolves_to_command_scope(
        transaction,
        &scope_type,
        &scope_id,
        "project",
        &input.task.project_id,
    )
    .await?
    {
        return Err(DbError::IdempotencyConflict);
    }

    // The Project binding is only one layer of the native authorization
    // ceiling. Re-read the source Agent's live identity and selected profile
    // while holding BEGIN IMMEDIATE so a revocation/profile rotation cannot
    // race the Task insert after adapter preflight.
    let identity = sqlx::query(
        "SELECT paused, archived_at, account_permission_ceiling,
                selected_profile_id
         FROM agent_identity WHERE id = ? LIMIT 1",
    )
    .bind(&actor_identity_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let paused: i64 = identity.try_get("paused")?;
    let archived_at: Option<String> = identity.try_get("archived_at")?;
    if paused != 0 || archived_at.is_some() {
        return Err(DbError::InvalidTransition);
    }
    let account_permission_ceiling: String = identity.try_get("account_permission_ceiling")?;
    if !permission_ceiling_contains(&account_permission_ceiling, "propose_task") {
        return Err(DbError::InvalidTransition);
    }
    let selected_profile_id: Option<String> = identity.try_get("selected_profile_id")?;
    let Some(selected_profile_id) = selected_profile_id else {
        return Err(DbError::InvalidTransition);
    };
    let profile_policy: Option<String> = sqlx::query_scalar(
        "SELECT tool_policy_json FROM agent_profile
         WHERE id = ? AND identity_id = ? LIMIT 1",
    )
    .bind(selected_profile_id)
    .bind(&actor_identity_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if !profile_policy
        .as_deref()
        .is_some_and(|value| permission_ceiling_contains(value, "propose_task"))
    {
        return Err(DbError::InvalidTransition);
    }

    let binding = sqlx::query(
        "SELECT permission_ceiling_json, policy_revision, policy_digest
         FROM project_agent_binding
         WHERE project_id = ? AND identity_id = ? AND state = 'active'
         LIMIT 1",
    )
    .bind(&input.task.project_id)
    .bind(&actor_identity_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(binding) = binding else {
        return Err(DbError::InvalidTransition);
    };
    let permission_ceiling: String = binding.try_get("permission_ceiling_json")?;
    let binding_policy_revision: String = binding.try_get("policy_revision")?;
    let binding_policy_digest: String = binding.try_get("policy_digest")?;
    if input
        .source_policy_revision
        .as_deref()
        .is_some_and(|revision| revision != binding_policy_revision)
        || input
            .source_policy_digest
            .as_deref()
            .is_some_and(|digest| digest != binding_policy_digest)
    {
        return Err(DbError::IdempotencyConflict);
    }
    let permits_task_proposal = !permission_ceiling.is_empty() && {
        let json = permission_ceiling.as_str();
        serde_json::from_str::<serde_json::Value>(json)
            .ok()
            .and_then(|value| {
                value
                    .get("allowed")
                    .or_else(|| value.get("permissions"))
                    .cloned()
            })
            .and_then(|value| value.as_array().cloned())
            .is_some_and(|permissions| {
                permissions
                    .iter()
                    .any(|permission| permission.as_str() == Some("propose_task"))
            })
    };
    if !permits_task_proposal {
        return Err(DbError::InvalidTransition);
    }

    match input.executor_type.as_str() {
        "agent" => {
            if input.executor_id != actor_identity_id {
                return Err(DbError::IdempotencyConflict);
            }
            let identity_exists: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM agent_identity WHERE id = ? LIMIT 1")
                    .bind(&input.executor_id)
                    .fetch_optional(&mut **transaction)
                    .await?;
            if identity_exists.is_none() {
                return Err(DbError::InvalidTransition);
            }
        }
        "user" => {
            let authorized: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project p
                 WHERE p.id = ?
                   AND (p.owner_id = ? OR EXISTS (
                       SELECT 1 FROM project_member pm
                       WHERE pm.project_id = p.id AND pm.user_id = ?
                   ))
                 LIMIT 1",
            )
            .bind(&input.task.project_id)
            .bind(&input.executor_id)
            .bind(&input.executor_id)
            .fetch_optional(&mut **transaction)
            .await?;
            if authorized.is_none() {
                return Err(DbError::InvalidTransition);
            }
        }
        _ => {
            return Err(DbError::Check(
                "task proposal executor type is unsupported".to_owned(),
            ));
        }
    }

    if let Some(execution) = input.action_execution.as_ref() {
        if Some(execution.action_id.as_str()) != input.source_action_id.as_deref()
            || Some(execution.expected_action_version) != input.expected_action_version
            || execution.executed_by_type != input.executor_type
            || execution.executed_by_id != input.executor_id
        {
            return Err(DbError::IdempotencyConflict);
        }
    }
    Ok(())
}

#[async_trait]
impl ProjectOrchestrationRepo for SqliteDb {
    async fn apply_adaptive_task_command(
        &self,
        input: ApplyAdaptiveTaskCommand,
    ) -> Result<AppliedAdaptiveTaskCommand> {
        super::task_adaptive::apply_adaptive_task_command(self, input).await
    }

    async fn insert_project_task_governance_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        governance: CreateProjectTaskGovernance,
    ) -> Result<()> {
        super::task_adaptive::insert_task_governance_in_tx(transaction, &governance).await
    }

    async fn create_task_proposal_command(
        &self,
        input: CreateTaskProposalCommand,
    ) -> Result<CreatedTaskProposal> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        let mut input = input;

        // Receipt lookup is deliberately the first operation.  A response
        // loss must replay the frozen Task even if the binding, baseline, or
        // action has since moved on; changed input/principal/version is
        // rejected by the canonical receipt identity before any current
        // authorization read can produce a different answer.
        if let Some(receipt) =
            resolve_command_replay(self, &mut transaction, input.command_receipt.as_ref()).await?
        {
            let task_id = command_outcome_string(&receipt, "task_id")?;
            let task_snapshot = command_outcome_task(&receipt)?;
            if task_snapshot.id != task_id || task_snapshot.project_id != receipt.scope_id {
                return Err(DbError::IdempotencyConflict);
            }
            // The receipt's persisted execution bundle is authoritative on
            // replay.  A concurrent retry has a newly minted candidate
            // execution id and (because the Task id is server-assigned) a
            // candidate outcome containing a different Task id; comparing
            // those transport candidates would turn an exact replay into a
            // false idempotency conflict.  The validator still proves that
            // the committed execution/action outcome exists and matches the
            // frozen receipt.
            validate_replay_action_bundle(&mut transaction, &receipt, None).await?;
            let persisted: Option<Task> = sqlx::query(&format!(
                "SELECT {TASK_COLUMNS} FROM task WHERE id = ? AND project_id = ?"
            ))
            .bind(&task_id)
            .bind(&receipt.scope_id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(map_task)
            .transpose()?;
            if persisted.is_none() {
                return Err(DbError::IdempotencyConflict);
            }
            transaction.commit().await?;
            return Ok(CreatedTaskProposal {
                task: task_snapshot,
                replayed: true,
            });
        }

        let receipt = input
            .command_receipt
            .as_ref()
            .ok_or_else(|| DbError::Check("task proposal requires a command receipt".to_owned()))?;
        if receipt.operation != "task.propose"
            || receipt.scope_type != "project"
            || receipt.scope_id != input.task.project_id
            || receipt.principal_type.trim().is_empty()
            || receipt.principal_id.trim().is_empty()
        {
            return Err(DbError::IdempotencyConflict);
        }
        if input.task.id.trim().is_empty()
            || input.task.title.trim().is_empty()
            || input.task.project_id.trim().is_empty()
        {
            return Err(DbError::Check(
                "Task proposal identity is incomplete".to_owned(),
            ));
        }
        if input.task.project_id != receipt.scope_id {
            return Err(DbError::IdempotencyConflict);
        }

        // The receipt lookup above is the replay boundary.  For a new
        // command, recheck source action admission, target/scope, active
        // binding permission, and executor ownership/member identity under
        // the same BEGIN IMMEDIATE transaction before any Task write.
        recheck_task_proposal_authorization_in_tx(&mut transaction, &input, receipt).await?;

        // Recheck parent ownership and allocate a subtask order while holding
        // the writer lock.  The service may have done a friendly preflight,
        // but this is the authority that closes the concurrent proposal race.
        if let Some(parent_task_id) = input.task.parent_task_id.as_deref() {
            let parent = sqlx::query(
                "SELECT project_id, parent_task_id, deleted_at
                 FROM task WHERE id = ? LIMIT 1",
            )
            .bind(parent_task_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let parent_project_id: String = parent.try_get("project_id")?;
            let parent_parent_id: Option<String> = parent.try_get("parent_task_id")?;
            let parent_deleted_at: Option<String> = parent.try_get("deleted_at")?;
            if parent_project_id != input.task.project_id
                || parent_parent_id.is_some()
                || parent_deleted_at.is_some()
            {
                return Err(DbError::InvalidTransition);
            }
        }

        input.governance = super::task_adaptive::validate_parent_proposal_in_tx(
            &mut transaction,
            &input.task,
            input.governance.take(),
        )
        .await?;

        // Dependencies are part of the proposal's authoritative acceptance,
        // not a best-effort follow-up. Re-authorize every prerequisite while
        // holding the same writer lock that will insert the Task and receipt.
        // The Task id is server-minted and not yet present, so a dependency
        // cycle cannot point back to it; duplicates are rejected explicitly.
        let mut seen_dependency_ids = std::collections::HashSet::new();
        for depends_on_task_id in &input.depends_on_task_ids {
            if depends_on_task_id.trim().is_empty()
                || depends_on_task_id.trim() != depends_on_task_id
                || !seen_dependency_ids.insert(depends_on_task_id.as_str())
            {
                return Err(DbError::Check(
                    "Task dependency ids must be non-empty and unique".to_owned(),
                ));
            }
            let prerequisite = sqlx::query(
                "SELECT project_id, status, deleted_at
                 FROM task WHERE id = ? LIMIT 1",
            )
            .bind(depends_on_task_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let prerequisite_project_id: String = prerequisite.try_get("project_id")?;
            let prerequisite_status: String = prerequisite.try_get("status")?;
            let prerequisite_deleted_at: Option<String> = prerequisite.try_get("deleted_at")?;
            if prerequisite_project_id != input.task.project_id
                || prerequisite_deleted_at.is_some()
                || prerequisite_status == "cancelled"
            {
                return Err(DbError::InvalidTransition);
            }
        }

        // The plan item is a Project-local singleton among non-cancelled
        // Tasks.  This check is inside the same transaction as the insert;
        // the old native read-before-create guard is intentionally not an
        // authority boundary.
        if let Some(governance) = input.governance.as_ref() {
            if governance.project_id != input.task.project_id || governance.task_id != input.task.id
            {
                return Err(DbError::IdempotencyConflict);
            }
            if let Some(plan_item_id) = governance.plan_item_id.as_deref() {
                let duplicate: Option<String> = sqlx::query_scalar(
                    "SELECT g.task_id
                     FROM project_task_governance g
                     JOIN task t ON t.id = g.task_id AND t.project_id = g.project_id
                     WHERE g.project_id = ? AND g.plan_item_id = ?
                       AND t.status <> 'cancelled' AND t.deleted_at IS NULL
                     ORDER BY g.created_at ASC, g.task_id ASC LIMIT 1",
                )
                .bind(&input.task.project_id)
                .bind(plan_item_id)
                .fetch_optional(&mut *transaction)
                .await?;
                if duplicate.is_some() {
                    return Err(DbError::Check(
                        "Task plan_item_id is already bound to a non-cancelled Task".to_owned(),
                    ));
                }
            }
            if governance.runnable {
                let Some(charter_revision_id) = governance.charter_revision_id.as_deref() else {
                    return Err(DbError::Check(
                        "runnable Task governance is missing its Charter reference".to_owned(),
                    ));
                };
                let admitted: i64 = sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1
                         FROM project p
                         WHERE p.id = ?
                           AND p.charter_status = 'charter_backed'
                           AND p.charter_setup_required = 0
                           AND p.current_charter_revision_id = ?
                     )",
                )
                .bind(&input.task.project_id)
                .bind(charter_revision_id)
                .fetch_one(&mut *transaction)
                .await?;
                if admitted != 1 {
                    return Err(DbError::InvalidTransition);
                }
            }
        }

        let mut create_task = input.task;
        if create_task.parent_task_id.is_some() {
            create_task.subtask_order = Some(
                sqlx::query_scalar(
                    "SELECT COALESCE(MAX(subtask_order) + 1, 0)
                     FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
                )
                .bind(create_task.parent_task_id.as_deref())
                .fetch_one(&mut *transaction)
                .await?,
            );
        }
        let task = TaskRepo::create_in_tx(self, &mut transaction, create_task).await?;
        if let Some(metadata_json) = input.metadata_json.as_deref() {
            sqlx::query(
                "UPDATE task SET metadata_json = ?, updated_at = ? WHERE id = ? AND project_id = ?",
            )
            .bind(metadata_json)
            .bind(&task.updated_at)
            .bind(&task.id)
            .bind(&task.project_id)
            .execute(&mut *transaction)
            .await?;
        }
        if !task.is_automation {
            ProjectRepo::increment_project_work_epoch(self, &mut transaction, &task.project_id, 1)
                .await?;
        }
        if let Some(governance) = input.governance {
            super::task_adaptive::insert_task_governance_in_tx(&mut transaction, &governance)
                .await?;
        }
        for depends_on_task_id in &input.depends_on_task_ids {
            sqlx::query(
                "INSERT INTO task_dependency (task_id, depends_on_id, created_at)
                 VALUES (?, ?, ?)",
            )
            .bind(&task.id)
            .bind(depends_on_task_id)
            .bind(&task.created_at)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?;
        }
        for assignment in &input.role_assignments {
            sqlx::query(
                "INSERT INTO task_role_assignment
                 (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(task_id, role_name) DO UPDATE SET
                    assignee_type = excluded.assignee_type,
                    assignee_id = excluded.assignee_id,
                    updated_at = excluded.updated_at",
            )
            .bind(&assignment.id)
            .bind(&assignment.task_id)
            .bind(&assignment.role_name)
            .bind(assignment.assignee_type.as_ref().map(ToString::to_string))
            .bind(assignment.assignee_id.as_deref())
            .bind(&assignment.created_at)
            .bind(&assignment.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?;
        }

        let frozen_task = map_task(
            sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
                .bind(&task.id)
                .fetch_one(&mut *transaction)
                .await?,
        )?;
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "task.created".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: frozen_task.id.clone(),
            actor_type: receipt.principal_type.clone(),
            actor_id: Some(receipt.principal_id.clone()),
            scope_type: receipt.scope_type.clone(),
            scope_id: receipt.scope_id.clone(),
            correlation_id: receipt.correlation_id.clone(),
            causation_id: receipt.causation_id.clone(),
            causation_depth: receipt.causation_depth,
            dedupe_key: Some(format!("task-created:{}", receipt.id)),
            payload_json: serde_json::json!({
                "task_id": frozen_task.id,
                "project_id": frozen_task.project_id,
                "title": frozen_task.title,
                "task_type": frozen_task.task_type,
                "status": frozen_task.status,
            })
            .to_string(),
            created_at: frozen_task.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;

        let mut receipt = input.command_receipt;
        let mut action_execution = input.action_execution;
        if let Some(receipt) = receipt.as_mut() {
            let mut outcome: serde_json::Value = serde_json::from_str(&receipt.outcome_json)
                .map_err(|_| DbError::IdempotencyConflict)?;
            outcome["task_id"] = serde_json::Value::String(frozen_task.id.clone());
            outcome["task"] = serde_json::to_value(&frozen_task).map_err(|error| {
                DbError::Check(format!(
                    "Task proposal outcome serialization failed: {error}"
                ))
            })?;
            receipt.outcome_json =
                serde_json::to_string(&outcome).map_err(|_| DbError::IdempotencyConflict)?;
            if let Some(action_execution) = action_execution.as_mut() {
                action_execution.result_json = Some(receipt.outcome_json.clone());
                action_execution.action_outcome_json = Some(receipt.outcome_json.clone());
            }
        }
        finalize_command_in_tx(self, &mut transaction, &event.id, receipt, action_execution)
            .await?;
        transaction.commit().await?;
        Ok(CreatedTaskProposal {
            task: frozen_task,
            replayed: false,
        })
    }

    async fn get_project_charter(&self, id: &str) -> Result<Option<ProjectCharterRecord>> {
        select_one(
            "SELECT * FROM project_charter WHERE id = ?",
            self.pool(),
            id,
            map_charter,
        )
        .await
    }

    async fn get_project_charter_by_project_id(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectCharterRecord>> {
        select_one(
            "SELECT * FROM project_charter
             WHERE project_id = ?",
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
        if let Some(receipt) =
            resolve_command_replay(self, &mut transaction, input.command_receipt.as_ref()).await?
        {
            let authoritative_revision_id = command_outcome_string(&receipt, "revision_id")?;
            validate_command_outcome_identity(
                &receipt,
                &[("charter_id", input.charter_id.as_str())],
            )?;
            validate_replay_action_bundle(
                &mut transaction,
                &receipt,
                input.action_execution.as_ref(),
            )
            .await?;
            let row = sqlx::query(
                "SELECT r.*, c.account_id, c.project_id
                 FROM project_charter_revision r
                 JOIN project_charter c ON c.id = r.charter_id
                 WHERE r.id = ? AND r.charter_id = ?",
            )
            .bind(&authoritative_revision_id)
            .bind(&input.charter_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_id: Option<String> = row.try_get("project_id")?;
            let account_id: String = row.try_get("account_id")?;
            if let Some(project_id) = project_id {
                validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
            } else {
                validate_command_scope(input.command_receipt.as_ref(), "account", &account_id)?;
            }
            let record = map_charter_revision(row)?;
            transaction.commit().await?;
            return Ok(record);
        }
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
            actor_type: input
                .command_receipt
                .as_ref()
                .map(|receipt| receipt.principal_type.clone())
                .unwrap_or_else(|| input.author_type.clone()),
            actor_id: input
                .command_receipt
                .as_ref()
                .map(|receipt| receipt.principal_id.clone())
                .or_else(|| input.author_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: charter_account_id,
            correlation_id: input
                .command_receipt
                .as_ref()
                .map(|receipt| receipt.correlation_id.clone())
                .unwrap_or_else(|| input.id.clone()),
            causation_id: input
                .command_receipt
                .as_ref()
                .and_then(|receipt| receipt.causation_id.clone())
                .or_else(|| input.source_message_id.clone()),
            causation_depth: input
                .command_receipt
                .as_ref()
                .map_or(0, |receipt| receipt.causation_depth),
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
        finalize_command_in_tx(
            self,
            &mut transaction,
            &event.id,
            input.command_receipt.clone(),
            input.action_execution.clone(),
        )
        .await?;
        transaction.commit().await?;
        self.get_project_charter_revision(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_project_charter_revision_atomically(
        &self,
        input: CreateProjectCharterRevisionAtomically,
    ) -> Result<ProjectCharterRevisionRecord> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        let command_receipt = input
            .command_receipt
            .as_ref()
            .or(input.revision.command_receipt.as_ref());
        let action_execution = input
            .action_execution
            .as_ref()
            .or(input.revision.action_execution.as_ref());
        if let Some(receipt) =
            resolve_command_replay(self, &mut transaction, command_receipt).await?
        {
            let authoritative_charter_id = command_outcome_string(&receipt, "charter_id")?;
            let authoritative_revision_id = command_outcome_string(&receipt, "revision_id")?;
            validate_replay_action_bundle(&mut transaction, &receipt, action_execution).await?;
            let row = sqlx::query(
                "SELECT r.*, c.account_id, c.project_id, c.genesis_session_id
                 FROM project_charter_revision r
                 JOIN project_charter c ON c.id = r.charter_id
                 WHERE r.id = ? AND r.charter_id = ?",
            )
            .bind(&authoritative_revision_id)
            .bind(&authoritative_charter_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_id: Option<String> = row.try_get("project_id")?;
            let account_id: String = row.try_get("account_id")?;
            if account_id != input.account_id
                || project_id.as_deref() != input.project_id.as_deref()
                || row
                    .try_get::<Option<String>, _>("genesis_session_id")?
                    .as_deref()
                    != input.genesis_session_id.as_deref()
            {
                return Err(DbError::VersionConflict);
            }
            if let Some(project_id) = project_id {
                validate_command_scope(command_receipt, "project", &project_id)?;
            } else {
                validate_command_scope(command_receipt, "account", &account_id)?;
            }
            let record = map_charter_revision(row)?;
            transaction.commit().await?;
            return Ok(record);
        }
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
            actor_type: command_receipt
                .map(|receipt| receipt.principal_type.clone())
                .unwrap_or_else(|| input.revision.author_type.clone()),
            actor_id: command_receipt
                .map(|receipt| receipt.principal_id.clone())
                .or_else(|| input.revision.author_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: charter_account_id,
            correlation_id: command_receipt
                .map(|receipt| receipt.correlation_id.clone())
                .unwrap_or_else(|| input.revision.id.clone()),
            causation_id: command_receipt
                .and_then(|receipt| receipt.causation_id.clone())
                .or_else(|| input.revision.source_message_id.clone()),
            causation_depth: command_receipt.map_or(0, |receipt| receipt.causation_depth),
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
        finalize_command_in_tx(
            self,
            &mut transaction,
            &event.id,
            command_receipt.cloned(),
            action_execution.cloned(),
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_charter_revision WHERE id = ?")
            .bind(&input.revision.id)
            .fetch_one(&mut *transaction)
            .await?;
        let record = map_charter_revision(row)?;
        transaction.commit().await?;
        Ok(record)
    }

    async fn finalize_project_charter_revision_noop(
        &self,
        input: FinalizeProjectCharterRevisionNoop,
    ) -> Result<ProjectCharterRevisionRecord> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        if let Some(receipt) =
            resolve_command_replay(self, &mut transaction, Some(&input.command_receipt)).await?
        {
            let authoritative_charter_id = command_outcome_string(&receipt, "charter_id")?;
            let authoritative_revision_id = command_outcome_string(&receipt, "revision_id")?;
            if authoritative_charter_id != input.charter_id
                || authoritative_revision_id != input.revision_id
            {
                return Err(DbError::IdempotencyConflict);
            }
            validate_replay_action_bundle(
                &mut transaction,
                &receipt,
                input.action_execution.as_ref(),
            )
            .await?;
            let row = sqlx::query(
                "SELECT r.*, c.account_id, c.project_id
                 FROM project_charter_revision r
                 JOIN project_charter c ON c.id = r.charter_id
                 WHERE r.id = ? AND r.charter_id = ?",
            )
            .bind(&authoritative_revision_id)
            .bind(&authoritative_charter_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let account_id: String = row.try_get("account_id")?;
            let project_id: Option<String> = row.try_get("project_id")?;
            if account_id != input.account_id
                || project_id.as_deref() != Some(input.project_id.as_str())
            {
                return Err(DbError::VersionConflict);
            }
            let content_digest: String = row.try_get("content_digest")?;
            let rendered_digest: String = row.try_get("rendered_digest")?;
            if content_digest != input.content_digest || rendered_digest != input.rendered_digest {
                return Err(DbError::IdempotencyConflict);
            }
            let record = map_charter_revision(row)?;
            transaction.commit().await?;
            return Ok(record);
        }

        let row = sqlx::query(
            "SELECT r.*, c.account_id, c.project_id, c.version AS charter_version,
                    c.current_draft_revision_id
             FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND r.charter_id = ?",
        )
        .bind(&input.revision_id)
        .bind(&input.charter_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let account_id: String = row.try_get("account_id")?;
        let project_id: Option<String> = row.try_get("project_id")?;
        let charter_version: i64 = row.try_get("charter_version")?;
        let current_draft_revision_id: Option<String> = row.try_get("current_draft_revision_id")?;
        let base_revision: i64 = row.try_get("base_revision")?;
        let base_revision_id: Option<String> = row.try_get("base_revision_id")?;
        let content_digest: String = row.try_get("content_digest")?;
        let rendered_digest: String = row.try_get("rendered_digest")?;
        if account_id != input.account_id
            || project_id.as_deref() != Some(input.project_id.as_str())
            || charter_version <= 1
            || current_draft_revision_id.as_deref() != Some(input.revision_id.as_str())
            || base_revision != 0
            || base_revision_id.is_some()
        {
            return Err(DbError::VersionConflict);
        }
        if content_digest != input.content_digest || rendered_digest != input.rendered_digest {
            return Err(DbError::IdempotencyConflict);
        }
        let outcome: serde_json::Value = serde_json::from_str(&input.command_receipt.outcome_json)
            .map_err(|_| DbError::IdempotencyConflict)?;
        for (key, expected) in [
            ("charter_id", input.charter_id.as_str()),
            ("revision_id", input.revision_id.as_str()),
        ] {
            if outcome.get(key).and_then(serde_json::Value::as_str) != Some(expected) {
                return Err(DbError::IdempotencyConflict);
            }
        }
        if outcome
            .get("charter_version")
            .and_then(serde_json::Value::as_i64)
            != Some(charter_version)
        {
            return Err(DbError::VersionConflict);
        }

        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project_charter.revision_noop".to_owned(),
            entity_type: "project_charter_revision".to_owned(),
            entity_id: input.revision_id.clone(),
            actor_type: input.command_receipt.principal_type.clone(),
            actor_id: Some(input.command_receipt.principal_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: account_id,
            correlation_id: input.command_receipt.correlation_id.clone(),
            causation_id: input.command_receipt.causation_id.clone(),
            causation_depth: input.command_receipt.causation_depth,
            dedupe_key: Some(format!(
                "project-charter-revision-noop:{}",
                input.command_receipt.id
            )),
            payload_json: serde_json::json!({
                "charter_id": input.charter_id,
                "revision_id": input.revision_id,
                "charter_version": charter_version,
                "content_digest": input.content_digest,
                "rendered_digest": input.rendered_digest,
                "semantic_noop": true,
            })
            .to_string(),
            created_at: input.command_receipt.committed_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        finalize_command_in_tx(
            self,
            &mut transaction,
            &event.id,
            Some(input.command_receipt),
            input.action_execution,
        )
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

    async fn apply_project_charter_approval_command(
        &self,
        input: ApplyProjectCharterApprovalCommand,
    ) -> Result<AppliedProjectCharterApprovalRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;

        // Resolve the durable command receipt before any mutable lifecycle,
        // version, binding, or approval validation.  A lost response must
        // replay even after the Project has moved to its new Charter state;
        // an altered principal/digest/key is rejected by the receipt lookup.
        if let Some(receipt) =
            resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?
        {
            validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
            validate_command_outcome_identity(
                &receipt,
                &[
                    ("project_id", input.project_id.as_str()),
                    ("charter_id", input.approval.charter_id.as_str()),
                    ("revision_id", input.approval.revision_id.as_str()),
                ],
            )?;
            let approval_id = command_outcome_string(&receipt, "approval_id")?;
            let binding_id =
                command_outcome_string_any(&receipt, &["project_agent_binding_id", "binding_id"])?;
            let chat_id = command_outcome_optional_string(&receipt, "project_chat_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let approval_row = sqlx::query("SELECT * FROM project_charter_approval WHERE id = ?")
                .bind(&approval_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::IdempotencyConflict)?;
            let approval = map_charter_approval(approval_row)?;
            let project_row = sqlx::query(
                "SELECT version, charter_status, charter_setup_required,
                        current_charter_id, current_charter_revision_id
                 FROM project WHERE id = ?",
            )
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_version: i64 = project_row.try_get("version")?;
            let project_charter_status: String = project_row.try_get("charter_status")?;
            let project_charter_setup_required: i64 =
                project_row.try_get("charter_setup_required")?;
            let project_charter_id: Option<String> = project_row.try_get("current_charter_id")?;
            let project_charter_revision_id: Option<String> =
                project_row.try_get("current_charter_revision_id")?;
            let binding_id: String = sqlx::query_scalar(
                "SELECT id FROM project_agent_binding
                 WHERE id = ? AND project_id = ?
                 LIMIT 1",
            )
            .bind(&binding_id)
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let chat_id: String = if let Some(chat_id) = chat_id {
                sqlx::query_scalar(
                    "SELECT id FROM agent_chat
                     WHERE id = ? AND kind = 'project' AND project_id = ?",
                )
                .bind(chat_id)
                .bind(&input.project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::IdempotencyConflict)?
            } else {
                sqlx::query_scalar(
                    "SELECT id FROM agent_chat
                     WHERE kind = 'project' AND project_id = ? LIMIT 1",
                )
                .bind(&input.project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::IdempotencyConflict)?
            };
            let outcome: serde_json::Value = serde_json::from_str(&receipt.outcome_json)
                .map_err(|_| DbError::IdempotencyConflict)?;
            let bootstrap_message_id = outcome
                .get("bootstrap_message_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let amendment_id = outcome
                .get("amendment_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            if let Some(message_id) = bootstrap_message_id.as_deref() {
                let exists: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM agent_chat_message
                     WHERE id = ? AND chat_id = ? LIMIT 1",
                )
                .bind(message_id)
                .bind(&chat_id)
                .fetch_optional(&mut *tx)
                .await?;
                if exists.is_none() {
                    return Err(DbError::IdempotencyConflict);
                }
            }
            if let Some(amendment_id) = amendment_id.as_deref() {
                let exists: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM project_charter_amendment
                     WHERE id = ? AND project_id = ? AND approval_id = ? LIMIT 1",
                )
                .bind(amendment_id)
                .bind(&input.project_id)
                .bind(&approval_id)
                .fetch_optional(&mut *tx)
                .await?;
                if exists.is_none() {
                    return Err(DbError::IdempotencyConflict);
                }
            }
            let project_charter_id = project_charter_id.ok_or(DbError::IdempotencyConflict)?;
            let project_charter_revision_id =
                project_charter_revision_id.ok_or(DbError::IdempotencyConflict)?;
            let record = AppliedProjectCharterApprovalRecord {
                approval,
                project_id: input.project_id,
                project_version,
                project_charter_status,
                project_charter_setup_required: project_charter_setup_required != 0,
                project_charter_id,
                project_charter_revision_id,
                project_agent_binding_id: binding_id,
                project_chat_id: chat_id,
                bootstrap_message_id,
                amendment_id,
            };
            tx.commit().await?;
            return Ok(record);
        }

        validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
        let approval = &input.approval;
        if !matches!(
            approval.approval_type.as_str(),
            "adoption" | "charter_amendment"
        ) {
            return Err(DbError::Check(
                "Project Charter command requires an adoption or amendment approval".to_owned(),
            ));
        }
        if approval.id.trim().is_empty()
            || approval.charter_id.trim().is_empty()
            || approval.revision_id.trim().is_empty()
            || approval.approving_principal_type.trim().is_empty()
            || approval.approving_principal_id.trim().is_empty()
            || approval.authorization_basis.trim().is_empty()
            || approval.authorization_action.trim().is_empty()
            || approval.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&approval.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        if approval.selected_identity_id.is_none()
            || approval.selected_profile_id.is_none()
            || approval.selected_operating_skill_revision_id.is_none()
            || approval.selected_policy_revision.is_none()
            || approval.selected_policy_digest.is_none()
        {
            return Err(DbError::VersionConflict);
        }

        let project = sqlx::query(
            "SELECT owner_id, name, charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id, version
             FROM project WHERE id = ?",
        )
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let project_owner_id: Option<String> = project.try_get("owner_id")?;
        let project_charter_status: String = project.try_get("charter_status")?;
        let project_charter_setup_required: i64 = project.try_get("charter_setup_required")?;
        let current_charter_id: Option<String> = project.try_get("current_charter_id")?;
        let current_charter_revision_id: Option<String> =
            project.try_get("current_charter_revision_id")?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.expected_project_version
            || approval.expected_charter_version < 1
        {
            return Err(DbError::VersionConflict);
        }
        let expected_approval_type = if project_charter_status == "legacy_unverified"
            && project_charter_setup_required != 0
            && current_charter_id.is_none()
            && current_charter_revision_id.is_none()
        {
            "adoption"
        } else if project_charter_status == "charter_backed"
            && project_charter_setup_required == 0
            && current_charter_id.is_some()
            && current_charter_revision_id.is_some()
        {
            "charter_amendment"
        } else {
            return Err(DbError::VersionConflict);
        };
        if approval.approval_type != expected_approval_type
            || input.expected_current_charter_revision_id != current_charter_revision_id
        {
            return Err(DbError::VersionConflict);
        }

        let charter = sqlx::query(
            "SELECT account_id, project_id, version, project_mode, maturity,
                    current_approved_revision_id
             FROM project_charter WHERE id = ?",
        )
        .bind(&approval.charter_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let account_id: String = charter.try_get("account_id")?;
        let charter_project_id: Option<String> = charter.try_get("project_id")?;
        if charter_project_id.as_deref() != Some(input.project_id.as_str()) {
            return Err(DbError::VersionConflict);
        }
        if project_owner_id
            .as_deref()
            .is_some_and(|owner| owner != account_id)
        {
            return Err(DbError::VersionConflict);
        }
        let charter_version: i64 = charter.try_get("version")?;
        let charter_project_mode: String = charter.try_get("project_mode")?;
        if charter_version != approval.expected_charter_version
            || charter_project_mode != approval.approved_project_mode
        {
            return Err(DbError::VersionConflict);
        }
        let previous_approved_revision_id: Option<String> =
            charter.try_get("current_approved_revision_id")?;
        if expected_approval_type == "charter_amendment"
            && previous_approved_revision_id.as_deref() != current_charter_revision_id.as_deref()
        {
            return Err(DbError::VersionConflict);
        }
        if expected_approval_type == "adoption" && previous_approved_revision_id.is_some() {
            return Err(DbError::VersionConflict);
        }
        let revision = sqlx::query(
            "SELECT lifecycle, charter_id, content_digest, rendered_digest
             FROM project_charter_revision
             WHERE id = ? AND charter_id = ?",
        )
        .bind(&approval.revision_id)
        .bind(&approval.charter_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let revision_lifecycle: String = revision.try_get("lifecycle")?;
        let revision_charter_id: String = revision.try_get("charter_id")?;
        let revision_content_digest: String = revision.try_get("content_digest")?;
        let revision_rendered_digest: String = revision.try_get("rendered_digest")?;
        if revision_charter_id != approval.charter_id
            || revision_content_digest != approval.content_digest
            || revision_rendered_digest != approval.rendered_digest
            || !matches!(
                revision_lifecycle.as_str(),
                "draft" | "proposed" | "approved"
            )
        {
            return Err(DbError::VersionConflict);
        }
        if expected_approval_type == "charter_amendment"
            && approval.revision_id == current_charter_revision_id.as_deref().unwrap_or_default()
        {
            return Err(DbError::VersionConflict);
        }

        let identity_id = approval
            .selected_identity_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let profile_id = approval
            .selected_profile_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let skill_revision_id = approval
            .selected_operating_skill_revision_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let selected = sqlx::query(
            "SELECT p.tool_policy_json, i.paused, i.archived_at,
                    i.selected_profile_id, sr.id AS skill_revision_id,
                    sr.skill_key, s.current_revision_id, s.lifecycle
             FROM agent_profile p
             JOIN agent_identity i ON i.id = p.identity_id
             JOIN operating_skill_revision sr ON sr.id = ?
             JOIN operating_skill s ON s.id = sr.operating_skill_id
             WHERE p.id = ? AND p.identity_id = ?
               AND i.owner_id = ? AND i.selected_profile_id = p.id
             LIMIT 1",
        )
        .bind(skill_revision_id)
        .bind(profile_id)
        .bind(identity_id)
        .bind(&account_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| DbError::VersionConflict)?;
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
            || selected_profile_id.as_deref() != Some(profile_id)
            || selected_skill_revision_id != skill_revision_id
            || selected_skill_current_revision_id.as_deref() != Some(skill_revision_id)
            || selected_skill_lifecycle != "active"
            || selected_skill_key != PROJECT_OPERATING_SKILL_KEY
            || approval.selected_policy_digest.as_deref() != Some(selected_policy_digest.as_str())
        {
            return Err(DbError::VersionConflict);
        }

        if expected_approval_type == "adoption" {
            let approved_name = approval
                .approved_name
                .as_deref()
                .filter(|name| !name.trim().is_empty())
                .ok_or(DbError::VersionConflict)?;
            let name_taken: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project
                 WHERE owner_id = ? AND name = ? AND id != ? LIMIT 1",
            )
            .bind(project_owner_id.as_deref().unwrap_or(account_id.as_str()))
            .bind(approved_name)
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?;
            if name_taken.is_some() {
                return Err(DbError::VersionConflict);
            }
        }

        // A prior active approval is an immutable receipt, so superseding it
        // records a lifecycle event rather than rewriting its target fields.
        let previous_active_approval: Option<String> = sqlx::query_scalar(
            "SELECT id FROM project_charter_approval
             WHERE charter_id = ? AND lifecycle = 'active' AND id != ? LIMIT 1",
        )
        .bind(&approval.charter_id)
        .bind(&approval.id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(previous_id) = previous_active_approval {
            let revoked = sqlx::query(
                "UPDATE project_charter_approval
                 SET lifecycle = 'revoked', version = version + 1, updated_at = ?
                 WHERE id = ? AND lifecycle = 'active'",
            )
            .bind(&approval.updated_at)
            .bind(&previous_id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if revoked.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
            sqlx::query(
                "INSERT INTO project_charter_approval_event (
                    id, approval_id, lifecycle, principal_type, principal_id,
                    authorization_basis, action, explicit_event, reason,
                    idempotency_key, occurred_at, created_at
                 ) VALUES (?, ?, 'revoked', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(new_uuid_v4())
            .bind(&previous_id)
            .bind(&approval.approving_principal_type)
            .bind(&approval.approving_principal_id)
            .bind(&approval.authorization_basis)
            .bind(&approval.authorization_action)
            .bind(&approval.explicit_event)
            .bind("Superseded by newer Project approval")
            .bind(format!(
                "{}:revoke:{}",
                approval.idempotency_key, previous_id
            ))
            .bind(&approval.authorization_occurred_at)
            .bind(&approval.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
        }

        if let Some(previous_revision_id) = previous_approved_revision_id.as_deref() {
            if previous_revision_id != approval.revision_id {
                let superseded = sqlx::query(
                    "UPDATE project_charter_revision SET lifecycle = 'superseded'
                     WHERE id = ? AND charter_id = ? AND lifecycle = 'approved'",
                )
                .bind(previous_revision_id)
                .bind(&approval.charter_id)
                .execute(&mut *tx)
                .await
                .map_err(orchestration_write_error)?;
                if superseded.rows_affected() != 1 {
                    return Err(DbError::VersionConflict);
                }
            }
        }
        let approved_revision = sqlx::query(
            "UPDATE project_charter_revision SET lifecycle = 'approved'
             WHERE id = ? AND charter_id = ? AND lifecycle != 'approved'",
        )
        .bind(&approval.revision_id)
        .bind(&approval.charter_id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if approved_revision.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let charter_update = sqlx::query(
            "UPDATE project_charter
             SET current_approved_revision_id = ?, lifecycle = 'attached',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(&approval.revision_id)
        .bind(&approval.updated_at)
        .bind(&approval.charter_id)
        .bind(&input.project_id)
        .bind(approval.expected_charter_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
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
                lifecycle, idempotency_key, version, created_at, updated_at,
                approved_project_mode, approval_event_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'active', ?, 1, ?, ?, ?, NULL)",
        )
        .bind(&approval.id)
        .bind(&approval.approval_type)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(&approval.content_digest)
        .bind(&approval.rendered_digest)
        .bind(approval.expected_charter_version)
        .bind(approval.approved_name.as_deref())
        .bind(approval.approved_slug.as_deref())
        .bind(approval.selected_identity_id.as_deref())
        .bind(approval.selected_profile_id.as_deref())
        .bind(approval.selected_operating_skill_revision_id.as_deref())
        .bind(approval.selected_policy_revision.as_deref())
        .bind(approval.selected_policy_digest.as_deref())
        .bind(&approval.approving_principal_type)
        .bind(&approval.approving_principal_id)
        .bind(&approval.authorization_basis)
        .bind(&approval.authorization_action)
        .bind(&approval.explicit_event)
        .bind(&approval.authorization_occurred_at)
        .bind(&approval.source_action)
        .bind(&approval.idempotency_key)
        .bind(&approval.created_at)
        .bind(&approval.updated_at)
        .bind(&approval.approved_project_mode)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        sqlx::query(
            "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, idempotency_key,
                occurred_at, created_at
             ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&approval.event_id)
        .bind(&approval.id)
        .bind(&approval.approving_principal_type)
        .bind(&approval.approving_principal_id)
        .bind(&approval.authorization_basis)
        .bind(&approval.authorization_action)
        .bind(&approval.explicit_event)
        .bind(format!("{}:active", approval.idempotency_key))
        .bind(&approval.authorization_occurred_at)
        .bind(&approval.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let linked = sqlx::query(
            "UPDATE project_charter_approval
             SET approval_event_id = ?
             WHERE id = ? AND lifecycle = 'active' AND approval_event_id IS NULL",
        )
        .bind(&approval.event_id)
        .bind(&approval.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if linked.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        let amendment_id = if expected_approval_type == "charter_amendment" {
            let base_revision_id = previous_approved_revision_id
                .clone()
                .ok_or(DbError::VersionConflict)?;
            let amendment_id = input.amendment_id.clone().ok_or(DbError::VersionConflict)?;
            let rationale = input
                .amendment_rationale
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or(DbError::VersionConflict)?;
            let material_diff_json = input
                .amendment_material_diff_json
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            let affected_records_json = input
                .amendment_affected_records_json
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            if serde_json::from_str::<serde_json::Value>(material_diff_json).is_err()
                || serde_json::from_str::<serde_json::Value>(affected_records_json).is_err()
            {
                return Err(DbError::Check(
                    "Charter amendment material diff and affected records must be JSON".to_owned(),
                ));
            }
            sqlx::query(
                "INSERT INTO project_charter_amendment (
                    id, project_id, base_charter_revision_id, candidate_revision_id,
                    lifecycle, rationale, material_diff_json, affected_records_json,
                    requested_principal_type, requested_principal_id,
                    expected_project_version, approval_id, version, created_at, updated_at
                 ) VALUES (?, ?, ?, ?, 'approved', ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
            )
            .bind(&amendment_id)
            .bind(&input.project_id)
            .bind(&base_revision_id)
            .bind(&approval.revision_id)
            .bind(rationale)
            .bind(material_diff_json)
            .bind(affected_records_json)
            .bind(&approval.approving_principal_type)
            .bind(&approval.approving_principal_id)
            .bind(input.expected_project_version)
            .bind(&approval.id)
            .bind(&approval.created_at)
            .bind(&approval.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            Some(amendment_id)
        } else {
            if input.amendment_id.is_some()
                || input.amendment_rationale.is_some()
                || input.amendment_material_diff_json.is_some()
                || input.amendment_affected_records_json.is_some()
            {
                return Err(DbError::VersionConflict);
            }
            None
        };

        let binding = sqlx::query(
            "SELECT id, state, version, autonomy_policy_json,
                    permission_ceiling_json, subscriptions_json, wake_budget
             FROM project_agent_binding
             WHERE id = ? AND project_id = ?
               AND state IN ('active', 'agent_setup_required')",
        )
        .bind(&input.existing_binding_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let binding_id: String = binding.try_get("id")?;
        let binding_state: String = binding.try_get("state")?;
        let binding_version: i64 = binding.try_get("version")?;
        let binding_policy: String = binding.try_get("autonomy_policy_json")?;
        let binding_ceiling: String = binding.try_get("permission_ceiling_json")?;
        let binding_subscriptions: String = binding.try_get("subscriptions_json")?;
        let binding_wake_budget: i64 = binding.try_get("wake_budget")?;
        let selected_policy_revision = approval
            .selected_policy_revision
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let selected_policy_digest = approval
            .selected_policy_digest
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let project_agent_binding_id = if binding_state == "agent_setup_required" {
            if input.replacement_binding_id.is_some() {
                return Err(DbError::VersionConflict);
            }
            let updated = sqlx::query(
                "UPDATE project_agent_binding
                 SET identity_id = ?, profile_id = ?, state = 'active',
                     permission_ceiling_json = ?,
                     operating_skill_revision_id = ?, policy_revision = ?, policy_digest = ?,
                     charter_id = ?, charter_revision_id = ?, charter_setup_required = 1,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ?
                   AND state = 'agent_setup_required' AND version = ?",
            )
            .bind(identity_id)
            .bind(profile_id)
            .bind(PROJECT_AGENT_PERMISSION_CEILING)
            .bind(skill_revision_id)
            .bind(selected_policy_revision)
            .bind(selected_policy_digest)
            .bind(&approval.charter_id)
            .bind(&approval.revision_id)
            .bind(&approval.updated_at)
            .bind(&binding_id)
            .bind(&input.project_id)
            .bind(binding_version)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if updated.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
            binding_id.clone()
        } else {
            let replacement_id = input
                .replacement_binding_id
                .clone()
                .unwrap_or_else(new_uuid_v4);
            if replacement_id.trim().is_empty() || replacement_id == binding_id {
                return Err(DbError::VersionConflict);
            }
            let replaced = sqlx::query(
                "UPDATE project_agent_binding
                 SET state = 'replaced', replacement_reason = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND state = 'active' AND version = ?",
            )
            .bind(if expected_approval_type == "charter_amendment" {
                "Project Charter amendment"
            } else {
                "Project Charter adoption"
            })
            .bind(&approval.updated_at)
            .bind(&binding_id)
            .bind(&input.project_id)
            .bind(binding_version)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if replaced.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
            sqlx::query(
                "INSERT INTO project_agent_binding (
                    id, project_id, identity_id, profile_id, state,
                    autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                    wake_budget, version, replaced_by_binding_id, replacement_reason,
                    operating_skill_revision_id, policy_revision, policy_digest,
                    charter_id, charter_revision_id, charter_setup_required,
                    created_at, updated_at
                 ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, NULL, NULL,
                           ?, ?, ?, ?, ?, 1, ?, ?)",
            )
            .bind(&replacement_id)
            .bind(&input.project_id)
            .bind(identity_id)
            .bind(profile_id)
            .bind(&binding_policy)
            .bind(&binding_ceiling)
            .bind(&binding_subscriptions)
            .bind(binding_wake_budget)
            .bind(binding_version)
            .bind(skill_revision_id)
            .bind(selected_policy_revision)
            .bind(selected_policy_digest)
            .bind(&approval.charter_id)
            .bind(&approval.revision_id)
            .bind(&approval.created_at)
            .bind(&approval.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            let linked = sqlx::query(
                "UPDATE project_agent_binding SET replaced_by_binding_id = ?
                 WHERE id = ? AND project_id = ? AND state = 'replaced'",
            )
            .bind(&replacement_id)
            .bind(&binding_id)
            .bind(&input.project_id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if linked.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
            replacement_id
        };

        let project_chat_id: String = sqlx::query_scalar(
            "SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?",
        )
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        sqlx::query(
            "UPDATE agent_chat SET status = 'ready', version = version + 1,
                    updated_at = ?
             WHERE id = ? AND kind = 'project' AND project_id = ?
               AND status = 'agent_setup_required'",
        )
        .bind(&approval.updated_at)
        .bind(&project_chat_id)
        .bind(&input.project_id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;

        let project_update = if expected_approval_type == "adoption" {
            let approved_name = approval
                .approved_name
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            sqlx::query(
                "UPDATE project
                 SET name = ?, charter_status = 'charter_backed',
                     charter_setup_required = 0, current_charter_id = ?,
                     current_charter_revision_id = ?, current_charter_version = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? AND charter_status = 'legacy_unverified'
                   AND charter_setup_required = 1
                   AND current_charter_id IS NULL AND current_charter_revision_id IS NULL",
            )
            .bind(approved_name)
            .bind(&approval.charter_id)
            .bind(&approval.revision_id)
            .bind(approval.expected_charter_version + 1)
            .bind(&approval.updated_at)
            .bind(&input.project_id)
            .bind(input.expected_project_version)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?
        } else {
            sqlx::query(
                "UPDATE project
                 SET current_charter_id = ?, current_charter_revision_id = ?,
                     current_charter_version = ?, version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = ? AND charter_status = 'charter_backed'
                   AND charter_setup_required = 0
                   AND current_charter_id = ? AND current_charter_revision_id = ?",
            )
            .bind(&approval.charter_id)
            .bind(&approval.revision_id)
            .bind(approval.expected_charter_version + 1)
            .bind(&approval.updated_at)
            .bind(&input.project_id)
            .bind(input.expected_project_version)
            .bind(&approval.charter_id)
            .bind(current_charter_revision_id.as_deref())
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?
        };
        if project_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        let bootstrap_message_id = if expected_approval_type == "adoption" {
            let message_id = input
                .bootstrap_message_id
                .clone()
                .ok_or(DbError::VersionConflict)?;
            let content = input
                .bootstrap_content
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            let guard = input
                .bootstrap_content_guard_json
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            let author_id = input
                .bootstrap_author_id
                .as_deref()
                .unwrap_or(approval.approving_principal_id.as_str());
            let correlation_id = input
                .bootstrap_correlation_id
                .as_deref()
                .unwrap_or(approval.idempotency_key.as_str());
            let source_metadata = input
                .bootstrap_source_metadata_json
                .as_deref()
                .unwrap_or("{}");
            if message_id.trim().is_empty()
                || content.trim().is_empty()
                || guard.trim().is_empty()
                || source_metadata.trim().is_empty()
            {
                return Err(DbError::Check(
                    "Project Charter adoption bootstrap message is incomplete".to_owned(),
                ));
            }
            let sequence: i64 = sqlx::query_scalar(
                "UPDATE agent_chat
                 SET message_count = message_count + 1, last_message_at = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND kind = 'project' AND project_id = ?
                 RETURNING message_count - 1",
            )
            .bind(&approval.updated_at)
            .bind(&approval.updated_at)
            .bind(&project_chat_id)
            .bind(&input.project_id)
            .fetch_one(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO agent_chat_message (
                    id, chat_id, sequence, author_type, author_id, content,
                    content_guard_json, sensitivity, status, correlation_id,
                    source_type, source_id, source_metadata_json, created_at
                 ) VALUES (?, ?, ?, 'system', ?, ?, ?, 'internal', 'complete', ?,
                           'native', ?, ?, ?)",
            )
            .bind(&message_id)
            .bind(&project_chat_id)
            .bind(sequence)
            .bind(author_id)
            .bind(content)
            .bind(guard)
            .bind(correlation_id)
            .bind(&approval.id)
            .bind(source_metadata)
            .bind(&approval.created_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            Some(message_id)
        } else {
            if input.bootstrap_message_id.is_some()
                || input.bootstrap_content.is_some()
                || input.bootstrap_content_guard_json.is_some()
                || input.bootstrap_author_id.is_some()
                || input.bootstrap_correlation_id.is_some()
                || input.bootstrap_source_metadata_json.is_some()
            {
                return Err(DbError::VersionConflict);
            }
            None
        };

        if expected_approval_type == "charter_amendment" {
            // A Charter amendment invalidates execution authorized by the
            // superseded Charter revision. The governance row's runnable
            // projection is the CAS-versioned exception to its immutability.
            sqlx::query(
                "UPDATE project_task_governance
                 SET runnable = 0, version = version + 1, updated_at = ?
                 WHERE project_id = ? AND runnable = 1
                   AND (charter_revision_id IS NULL OR charter_revision_id != ?)",
            )
            .bind(&approval.updated_at)
            .bind(&input.project_id)
            .bind(&approval.revision_id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
        }

        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                approval.approving_principal_type.clone(),
                Some(approval.approving_principal_id.clone()),
                approval.idempotency_key.clone(),
                Some(approval.event_id.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.charter.approved".to_owned(),
            entity_type: "project_charter".to_owned(),
            entity_id: approval.charter_id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!("project-charter-approval:{}", approval.id)),
            payload_json: serde_json::json!({
                "project_id": input.project_id.clone(),
                "charter_id": approval.charter_id.clone(),
                "revision_id": approval.revision_id.clone(),
                "approval_id": approval.id.clone(),
                "approval_type": approval.approval_type.clone(),
                "content_digest": approval.content_digest.clone(),
                "rendered_digest": approval.rendered_digest.clone(),
                "bootstrap_message_id": bootstrap_message_id.clone(),
                "amendment_id": amendment_id.clone(),
            })
            .to_string(),
            created_at: approval.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;

        let consumed = sqlx::query(
            "UPDATE project_charter_approval
             SET lifecycle = 'consumed', consumed_project_id = ?, consumed_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(&input.project_id)
        .bind(&approval.updated_at)
        .bind(&approval.updated_at)
        .bind(&approval.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if consumed.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, reason,
                idempotency_key, occurred_at, created_at
             ) VALUES (?, ?, 'consumed', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&approval.id)
        .bind(&approval.approving_principal_type)
        .bind(&approval.approving_principal_id)
        .bind(&approval.authorization_basis)
        .bind(&approval.authorization_action)
        .bind(&approval.explicit_event)
        .bind(if expected_approval_type == "adoption" {
            "Project Charter adoption applied"
        } else {
            "Project Charter amendment applied"
        })
        .bind(format!("{}:consumed", approval.idempotency_key))
        .bind(&approval.authorization_occurred_at)
        .bind(&approval.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;

        let admission_receipt_id = if expected_approval_type == "adoption" {
            let receipt_id = new_uuid_v4();
            sqlx::query(
                "INSERT INTO project_admission_receipt (
                    id, project_id, source_kind, handoff_id,
                    initial_charter_approval_id, initial_charter_id,
                    initial_charter_revision_id, payload_digest,
                    validation_schema_version, validated_at, created_at
                 ) VALUES (?, ?, 'charter_adoption', NULL, ?, ?, ?, ?,
                           'forge.project-admission/v1', ?, ?)",
            )
            .bind(&receipt_id)
            .bind(&input.project_id)
            .bind(&approval.id)
            .bind(&approval.charter_id)
            .bind(&approval.revision_id)
            .bind(&approval.content_digest)
            .bind(&approval.updated_at)
            .bind(&approval.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            receipt_id
        } else {
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM project_admission_receipt
                 WHERE project_id = ? LIMIT 1",
            )
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::VersionConflict)?
        };
        let completed_binding = sqlx::query(
            "UPDATE project_agent_binding
             SET admission_receipt_id = ?, charter_approval_id = ?,
                 charter_setup_required = 0
             WHERE id = ? AND project_id = ? AND state = 'active'
               AND charter_setup_required = 1",
        )
        .bind(&admission_receipt_id)
        .bind(&approval.id)
        .bind(&project_agent_binding_id)
        .bind(&input.project_id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if completed_binding.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt.clone(),
            input.action_execution.clone(),
        )
        .await?;

        let approval_row = sqlx::query("SELECT * FROM project_charter_approval WHERE id = ?")
            .bind(&approval.id)
            .fetch_one(&mut *tx)
            .await?;
        let project_row = sqlx::query(
            "SELECT version, charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ?",
        )
        .bind(&input.project_id)
        .fetch_one(&mut *tx)
        .await?;
        let record = AppliedProjectCharterApprovalRecord {
            approval: map_charter_approval(approval_row)?,
            project_id: input.project_id.clone(),
            project_version: project_row.try_get("version")?,
            project_charter_status: project_row.try_get("charter_status")?,
            project_charter_setup_required: project_row
                .try_get::<i64, _>("charter_setup_required")?
                != 0,
            project_charter_id: project_row
                .try_get::<Option<String>, _>("current_charter_id")?
                .ok_or(DbError::VersionConflict)?,
            project_charter_revision_id: project_row
                .try_get::<Option<String>, _>("current_charter_revision_id")?
                .ok_or(DbError::VersionConflict)?,
            project_agent_binding_id,
            project_chat_id,
            bootstrap_message_id,
            amendment_id,
        };
        tx.commit().await?;
        Ok(record)
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

    async fn create_project_document_shell_command(
        &self,
        input: CreateProjectDocumentShellCommand,
    ) -> Result<ProjectDocumentRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                [("project_id", input.document.project_id.as_str())].as_ref(),
            )?;
            let document_id = command_outcome_string(&receipt, "document_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT * FROM project_document
                 WHERE id = ? AND project_id = ?",
            )
            .bind(&document_id)
            .bind(&input.document.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let document = map_document(row)?;
            tx.commit().await?;
            return Ok(document);
        }

        if input.expected_project_version < 1 {
            return Err(DbError::VersionConflict);
        }
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &input.document.project_id,
        )?;
        let now = input.document.updated_at.clone();
        let updated = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&now)
        .bind(&input.document.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
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

        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                "user".to_owned(),
                None,
                input.document.id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.document.created".to_owned(),
            entity_type: "project_document".to_owned(),
            entity_id: input.document.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.document.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!("project-document-created:{}", input.document.id)),
            payload_json: serde_json::json!({
                "project_id": input.document.project_id.clone(),
                "document_id": input.document.id.clone(),
                "kind": input.document.kind.clone(),
                "title": input.document.title.clone(),
                "approval_policy": input.document.approval_policy.clone(),
                "expected_project_version": input.expected_project_version,
            })
            .to_string(),
            created_at: input.document.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_document WHERE id = ?")
            .bind(&input.document.id)
            .fetch_one(&mut *tx)
            .await?;
        let document = map_document(row)?;
        tx.commit().await?;
        Ok(document)
    }

    async fn create_project_document_command(
        &self,
        input: CreateProjectDocumentCommand,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("project_id", input.document.project_id.as_str())],
            )?;
            let document_id = command_outcome_string(&receipt, "document_id")?;
            let revision_id = command_outcome_string(&receipt, "revision_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT r.*, d.project_id
                 FROM project_document_revision r
                 JOIN project_document d ON d.id = r.document_id
                 WHERE r.id = ? AND r.document_id = ?",
            )
            .bind(&revision_id)
            .bind(&document_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_id: String = row.try_get("project_id")?;
            validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
            let record = map_document_revision(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        if input.document.id != input.revision.document_id
            || input.revision.expected_document_version != 1
            || input.revision.base_revision != 0
            || input.revision.base_revision_id.is_some()
        {
            return Err(DbError::VersionConflict);
        }
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &input.document.project_id,
        )?;
        sqlx::query("SELECT id FROM project WHERE id = ?")
            .bind(&input.document.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
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
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                input.revision.author_type.clone(),
                input.revision.author_id.clone(),
                input.revision.id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.document.revision_created".to_owned(),
            entity_type: "project_document_revision".to_owned(),
            entity_id: input.revision.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.document.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
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
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_document_revision WHERE id = ?")
            .bind(&input.revision.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_document_revision(row)
    }

    async fn append_project_document_revision_command(
        &self,
        input: AppendProjectDocumentRevisionCommand,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("document_id", input.revision.document_id.as_str())],
            )?;
            let revision_id = command_outcome_string(&receipt, "revision_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT r.*, d.project_id
                 FROM project_document_revision r
                 JOIN project_document d ON d.id = r.document_id
                 WHERE r.id = ? AND r.document_id = ?",
            )
            .bind(&revision_id)
            .bind(&input.revision.document_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_id: String = row.try_get("project_id")?;
            validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
            let record = map_document_revision(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        let document = sqlx::query(
            "SELECT project_id, version, current_draft_revision_id
             FROM project_document WHERE id = ?",
        )
        .bind(&input.revision.document_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let project_id: String = document.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let document_version: i64 = document.try_get("version")?;
        if document_version != input.revision.expected_document_version {
            return Err(DbError::VersionConflict);
        }
        let current_draft: Option<String> = document.try_get("current_draft_revision_id")?;
        if input.revision.base_revision > 0 {
            let Some(current_draft) = current_draft else {
                return Err(DbError::VersionConflict);
            };
            let Some(base_revision_id) = input.revision.base_revision_id.as_deref() else {
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
            .bind(&input.revision.document_id)
            .bind(input.revision.base_revision)
            .fetch_optional(&mut *tx)
            .await?;
            if base_matches.is_none() {
                return Err(DbError::VersionConflict);
            }
        } else if input.revision.base_revision_id.is_some() || current_draft.is_some() {
            return Err(DbError::VersionConflict);
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_document_revision WHERE document_id = ?",
        )
        .bind(&input.revision.document_id)
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
        .bind(&input.revision.id)
        .bind(&input.revision.document_id)
        .bind(revision)
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
             WHERE id = ? AND version = ?",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.created_at)
        .bind(&input.revision.document_id)
        .bind(input.revision.expected_document_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                input.revision.author_type.clone(),
                input.revision.author_id.clone(),
                input.revision.id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.document.revision_created".to_owned(),
            entity_type: "project_document_revision".to_owned(),
            entity_id: input.revision.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-document-revision-created:{}",
                input.revision.id
            )),
            payload_json: serde_json::json!({
                "document_id": input.revision.document_id.clone(),
                "revision_id": input.revision.id.clone(),
                "revision": revision,
                "lifecycle": input.revision.lifecycle.clone(),
                "content_digest": input.revision.content_digest.clone(),
                "rendered_digest": input.revision.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.revision.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
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

    async fn approve_project_document_command(
        &self,
        input: ApproveProjectDocumentCommand,
    ) -> Result<ProjectDocumentApprovalRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[
                    ("document_id", input.approval.document_id.as_str()),
                    ("revision_id", input.approval.revision_id.as_str()),
                ],
            )?;
            let approval_id = command_outcome_string(&receipt, "approval_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT a.*, d.project_id
                 FROM project_document_approval a
                 JOIN project_document d ON d.id = a.document_id
                 WHERE a.id = ? AND a.document_id = ? AND a.revision_id = ?",
            )
            .bind(&approval_id)
            .bind(&input.approval.document_id)
            .bind(&input.approval.revision_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_id: String = row.try_get("project_id")?;
            validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
            let record = map_document_approval(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        let approval = &input.approval;
        if approval.principal_type.trim().is_empty()
            || approval.principal_id.trim().is_empty()
            || approval.authorization_basis.trim().is_empty()
            || approval.authorization_action.trim().is_empty()
            || approval.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&approval.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let document = sqlx::query(
            "SELECT project_id, version, current_draft_revision_id
             FROM project_document WHERE id = ?",
        )
        .bind(&approval.document_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let project_id: String = document.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let document_version: i64 = document.try_get("version")?;
        let current_draft_revision_id: Option<String> =
            document.try_get("current_draft_revision_id")?;
        if document_version != approval.expected_document_version {
            return Err(DbError::VersionConflict);
        }
        if current_draft_revision_id.as_deref() != Some(approval.revision_id.as_str()) {
            return Err(DbError::VersionConflict);
        }
        let target = sqlx::query(
            "SELECT lifecycle FROM project_document_revision
             WHERE id = ? AND document_id = ? AND content_digest = ?
               AND rendered_digest = ?",
        )
        .bind(&approval.revision_id)
        .bind(&approval.document_id)
        .bind(&approval.content_digest)
        .bind(&approval.rendered_digest)
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
        .bind(&approval.document_id)
        .bind(&approval.revision_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        let approved = sqlx::query(
            "UPDATE project_document_revision SET lifecycle = 'approved'
             WHERE id = ? AND document_id = ? AND lifecycle != 'approved'",
        )
        .bind(&approval.revision_id)
        .bind(&approval.document_id)
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
        .bind(&approval.revision_id)
        .bind(&approval.updated_at)
        .bind(&approval.document_id)
        .bind(approval.expected_document_version)
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
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?)",
        )
        .bind(&approval.id)
        .bind(&approval.document_id)
        .bind(&approval.revision_id)
        .bind(&approval.principal_type)
        .bind(&approval.principal_id)
        .bind(&approval.authorization_basis)
        .bind(&approval.authorization_action)
        .bind(&approval.explicit_event)
        .bind(&approval.authorization_occurred_at)
        .bind(&approval.content_digest)
        .bind(&approval.rendered_digest)
        .bind(&approval.idempotency_key)
        .bind(&approval.created_at)
        .bind(&approval.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                approval.principal_type.clone(),
                Some(approval.principal_id.clone()),
                approval.id.clone(),
                Some(approval.explicit_event.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.document.approved".to_owned(),
            entity_type: "project_document_approval".to_owned(),
            entity_id: approval.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!("project-document-approved:{}", approval.id)),
            payload_json: serde_json::json!({
                "document_id": approval.document_id.clone(),
                "revision_id": approval.revision_id.clone(),
                "approval_id": approval.id.clone(),
                "content_digest": approval.content_digest.clone(),
                "rendered_digest": approval.rendered_digest.clone(),
            })
            .to_string(),
            created_at: approval.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_document_approval WHERE id = ?")
            .bind(&approval.id)
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

    async fn create_project_decision_candidate_command(
        &self,
        input: CreateProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("project_id", input.candidate.project_id.as_str())],
            )?;
            let candidate_id = command_outcome_string(&receipt, "candidate_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT c.*, p.id AS project_exists
                 FROM project_decision_candidate c
                 JOIN project p ON p.id = c.project_id
                 WHERE c.id = ? AND c.project_id = ?",
            )
            .bind(&candidate_id)
            .bind(&input.candidate.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(
                input.command_receipt.as_ref(),
                "project",
                &input.candidate.project_id,
            )?;
            let record = map_decision_candidate(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        let candidate = &input.candidate;
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &candidate.project_id,
        )?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&candidate.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != candidate.expected_project_version {
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
        .bind(&candidate.id)
        .bind(&candidate.project_id)
        .bind(&candidate.lifecycle)
        .bind(&candidate.question)
        .bind(&candidate.context_json)
        .bind(&candidate.options_json)
        .bind(candidate.selected_outcome.as_deref())
        .bind(candidate.rationale.as_deref())
        .bind(candidate.principal_type.as_deref())
        .bind(candidate.principal_id.as_deref())
        .bind(&candidate.source_refs_json)
        .bind(candidate.expected_project_version)
        .bind(&candidate.created_at)
        .bind(&candidate.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let advanced = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&candidate.updated_at)
        .bind(&candidate.project_id)
        .bind(candidate.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let default_actor_type = candidate
            .principal_type
            .clone()
            .unwrap_or_else(|| "system".to_owned());
        let default_actor_id = candidate.principal_id.clone();
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                default_actor_type,
                default_actor_id,
                candidate.id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.decision.candidate_created".to_owned(),
            entity_type: "project_decision_candidate".to_owned(),
            entity_id: candidate.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: candidate.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-decision-candidate-created:{}",
                candidate.id
            )),
            payload_json: serde_json::json!({
                "project_id": candidate.project_id.clone(),
                "candidate_id": candidate.id.clone(),
                "lifecycle": candidate.lifecycle.clone(),
            })
            .to_string(),
            created_at: candidate.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_decision_candidate WHERE id = ?")
            .bind(&candidate.id)
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
                authorization_occurred_at, charter_revision_id,
                source_refs_json, affected_records_json, supersedes_decision_id,
                created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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

    async fn append_project_decision_command(
        &self,
        input: AppendProjectDecisionCommand,
    ) -> Result<ProjectDecisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("project_id", input.decision.project_id.as_str())],
            )?;
            let decision_id = command_outcome_string(&receipt, "decision_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT * FROM project_decision
                 WHERE id = ? AND project_id = ?",
            )
            .bind(&decision_id)
            .bind(&input.decision.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(
                input.command_receipt.as_ref(),
                "project",
                &input.decision.project_id,
            )?;
            let record = map_decision(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        let decision = &input.decision;
        if decision.principal_type.trim().is_empty()
            || decision.principal_id.trim().is_empty()
            || decision.authority_basis.trim().is_empty()
            || decision.authorization_action.trim().is_empty()
            || decision.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&decision.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &decision.project_id,
        )?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&decision.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != decision.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        if let Some(supersedes_id) = decision.supersedes_decision_id.as_deref() {
            let belongs: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_decision
                 WHERE id = ? AND project_id = ? LIMIT 1",
            )
            .bind(supersedes_id)
            .bind(&decision.project_id)
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
                authorization_occurred_at, charter_revision_id,
                source_refs_json, affected_records_json, supersedes_decision_id,
                created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&decision.id)
        .bind(&decision.project_id)
        .bind(&decision.state)
        .bind(&decision.decision_class)
        .bind(&decision.question)
        .bind(&decision.context_json)
        .bind(&decision.options_json)
        .bind(&decision.selected_outcome)
        .bind(&decision.rationale)
        .bind(&decision.principal_type)
        .bind(&decision.principal_id)
        .bind(&decision.authority_basis)
        .bind(&decision.authorization_action)
        .bind(&decision.explicit_event)
        .bind(&decision.authorization_occurred_at)
        .bind(decision.charter_revision_id.as_deref())
        .bind(&decision.source_refs_json)
        .bind(&decision.affected_records_json)
        .bind(decision.supersedes_decision_id.as_deref())
        .bind(&decision.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let advanced = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&decision.created_at)
        .bind(&decision.project_id)
        .bind(decision.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                decision.principal_type.clone(),
                Some(decision.principal_id.clone()),
                decision.id.clone(),
                Some(decision.explicit_event.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.decision.created".to_owned(),
            entity_type: "project_decision".to_owned(),
            entity_id: decision.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: decision.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!("project-decision-created:{}", decision.id)),
            payload_json: serde_json::json!({
                "project_id": decision.project_id.clone(),
                "decision_id": decision.id.clone(),
                "state": decision.state.clone(),
                "decision_class": decision.decision_class.clone(),
                "supersedes_decision_id": decision.supersedes_decision_id.clone(),
                "charter_revision_id": decision.charter_revision_id.clone(),
            })
            .to_string(),
            created_at: decision.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_decision WHERE id = ?")
            .bind(&decision.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_decision(row)
    }

    async fn approve_project_decision_candidate_command(
        &self,
        input: ApproveProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("candidate_id", input.candidate_id.as_str())],
            )?;
            let decision_id = command_outcome_string(&receipt, "decision_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let candidate_exists: Option<String> = sqlx::query_scalar(
                "SELECT id FROM project_decision_candidate
                 WHERE id = ? AND project_id = ? AND effective_decision_id = ?",
            )
            .bind(&input.candidate_id)
            .bind(&input.decision.project_id)
            .bind(&decision_id)
            .fetch_optional(&mut *tx)
            .await?;
            if candidate_exists.is_none() {
                return Err(DbError::IdempotencyConflict);
            }
            let row = sqlx::query("SELECT * FROM project_decision WHERE id = ? AND project_id = ?")
                .bind(&decision_id)
                .bind(&input.decision.project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(
                input.command_receipt.as_ref(),
                "project",
                &input.decision.project_id,
            )?;
            let record = map_decision(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        let decision = &input.decision;
        if decision.principal_type.trim().is_empty()
            || decision.principal_id.trim().is_empty()
            || decision.authority_basis.trim().is_empty()
            || decision.authorization_action.trim().is_empty()
            || decision.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&decision.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &decision.project_id,
        )?;
        let project =
            sqlx::query("SELECT version, current_charter_revision_id FROM project WHERE id = ?")
                .bind(&decision.project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != decision.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        let candidate = sqlx::query(
            "SELECT * FROM project_decision_candidate
             WHERE id = ? AND project_id = ?",
        )
        .bind(&input.candidate_id)
        .bind(&decision.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let candidate_lifecycle: String = candidate.try_get("lifecycle")?;
        if !matches!(candidate_lifecycle.as_str(), "draft" | "proposed") {
            return Err(DbError::VersionConflict);
        }
        let candidate_version: i64 = candidate.try_get("version")?;
        if candidate_version != input.expected_candidate_version {
            return Err(DbError::VersionConflict);
        }
        let candidate_question: String = candidate.try_get("question")?;
        let candidate_context_json: String = candidate.try_get("context_json")?;
        let candidate_options_json: String = candidate.try_get("options_json")?;
        let candidate_selected_outcome: Option<String> = candidate.try_get("selected_outcome")?;
        let candidate_rationale: Option<String> = candidate.try_get("rationale")?;
        let candidate_source_refs_json: String = candidate.try_get("source_refs_json")?;
        if candidate_question != decision.question
            || candidate_context_json != decision.context_json
            || candidate_options_json != decision.options_json
            || candidate_selected_outcome.as_deref() != Some(decision.selected_outcome.as_str())
            || candidate_rationale.as_deref() != Some(decision.rationale.as_str())
            || candidate_source_refs_json != decision.source_refs_json
        {
            return Err(DbError::VersionConflict);
        }
        let context: serde_json::Value = serde_json::from_str(&candidate_context_json)
            .map_err(|_| DbError::Check("Decision candidate context is invalid".to_owned()))?;
        if let Some(context_class) = context.get("decision_class").and_then(|v| v.as_str()) {
            if context_class != decision.decision_class {
                return Err(DbError::VersionConflict);
            }
        }
        let context_supersedes = context
            .get("supersedes_decision_id")
            .and_then(|v| v.as_str())
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned);
        let context_invalidates = context
            .get("invalidates_decision_id")
            .and_then(|v| v.as_str())
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned);
        if context_supersedes.is_some() && context_invalidates.is_some() {
            return Err(DbError::VersionConflict);
        }
        let expected_state = if context_invalidates.is_some() {
            "invalidated"
        } else {
            "active"
        };
        if decision.state != expected_state
            || decision.supersedes_decision_id
                != context_supersedes.clone().or(context_invalidates.clone())
        {
            return Err(DbError::VersionConflict);
        }
        if let Some(target_id) = decision.supersedes_decision_id.as_deref() {
            let target_exists: Option<String> = sqlx::query_scalar(
                "SELECT id FROM project_decision WHERE id = ? AND project_id = ?",
            )
            .bind(target_id)
            .bind(&decision.project_id)
            .fetch_optional(&mut *tx)
            .await?;
            if target_exists.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        sqlx::query(
            "INSERT INTO project_decision (
                id, project_id, state, decision_class, question, context_json,
                options_json, selected_outcome, rationale, principal_type,
                principal_id, authority_basis, authorization_action, explicit_event,
                authorization_occurred_at, charter_revision_id,
                source_refs_json, affected_records_json, supersedes_decision_id,
                created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&decision.id)
        .bind(&decision.project_id)
        .bind(&decision.state)
        .bind(&decision.decision_class)
        .bind(&decision.question)
        .bind(&decision.context_json)
        .bind(&decision.options_json)
        .bind(&decision.selected_outcome)
        .bind(&decision.rationale)
        .bind(&decision.principal_type)
        .bind(&decision.principal_id)
        .bind(&decision.authority_basis)
        .bind(&decision.authorization_action)
        .bind(&decision.explicit_event)
        .bind(&decision.authorization_occurred_at)
        .bind(decision.charter_revision_id.as_deref())
        .bind(&decision.source_refs_json)
        .bind(&decision.affected_records_json)
        .bind(decision.supersedes_decision_id.as_deref())
        .bind(&decision.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let candidate_update = sqlx::query(
            "UPDATE project_decision_candidate
             SET lifecycle = 'approved', effective_decision_id = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('draft', 'proposed')",
        )
        .bind(&decision.id)
        .bind(&decision.created_at)
        .bind(&input.candidate_id)
        .bind(&decision.project_id)
        .bind(input.expected_candidate_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if candidate_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let project_update = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&decision.created_at)
        .bind(&decision.project_id)
        .bind(decision.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if project_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                decision.principal_type.clone(),
                Some(decision.principal_id.clone()),
                decision.id.clone(),
                Some(decision.explicit_event.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.decision.approved".to_owned(),
            entity_type: "project_decision".to_owned(),
            entity_id: decision.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: decision.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-decision-approved:{}:{}",
                input.candidate_id, decision.id
            )),
            payload_json: serde_json::json!({
                "project_id": decision.project_id.clone(),
                "candidate_id": input.candidate_id.clone(),
                "decision_id": decision.id.clone(),
                "expected_project_version": decision.expected_project_version,
                "principal_id": decision.principal_id.clone(),
                "authorization_event_id": decision.explicit_event.clone(),
                "authorization_basis": decision.authority_basis.clone(),
            })
            .to_string(),
            created_at: decision.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_decision WHERE id = ?")
            .bind(&decision.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_decision(row)
    }

    async fn reject_project_decision_candidate_command(
        &self,
        input: RejectProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("candidate_id", input.candidate_id.as_str())],
            )?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT * FROM project_decision_candidate
                 WHERE id = ? AND project_id = ? AND lifecycle = 'rejected'",
            )
            .bind(&input.candidate_id)
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
            let record = map_decision_candidate(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        if input.reason.trim().is_empty()
            || input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        let candidate = sqlx::query(
            "SELECT * FROM project_decision_candidate
             WHERE id = ? AND project_id = ?",
        )
        .bind(&input.candidate_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let lifecycle: String = candidate.try_get("lifecycle")?;
        if !matches!(lifecycle.as_str(), "draft" | "proposed") {
            return Err(DbError::VersionConflict);
        }
        let candidate_version: i64 = candidate.try_get("version")?;
        if candidate_version != input.expected_candidate_version {
            return Err(DbError::VersionConflict);
        }
        let mut context: serde_json::Value =
            serde_json::from_str(&candidate.try_get::<String, _>("context_json")?)
                .map_err(|_| DbError::Check("Decision candidate context is invalid".to_owned()))?;
        if !context.is_object() {
            context = serde_json::json!({"summary": context});
        }
        context["rejection_reason"] = serde_json::Value::String(input.reason.clone());
        let context_json = serde_json::to_string(&context)
            .map_err(|_| DbError::Check("Decision candidate context is invalid".to_owned()))?;
        let updated = sqlx::query(
            "UPDATE project_decision_candidate
             SET lifecycle = 'rejected', context_json = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('draft', 'proposed')",
        )
        .bind(&context_json)
        .bind(&input.updated_at)
        .bind(&input.candidate_id)
        .bind(&input.project_id)
        .bind(input.expected_candidate_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let project_update = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.updated_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if project_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                input.principal_type.clone(),
                Some(input.principal_id.clone()),
                input.candidate_id.clone(),
                Some(input.explicit_event.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.decision.candidate_rejected".to_owned(),
            entity_type: "project_decision_candidate".to_owned(),
            entity_id: input.candidate_id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-decision-rejected:{}:{}",
                input.candidate_id,
                input.command_receipt.as_ref().map_or_else(
                    || input.explicit_event.clone(),
                    |receipt| receipt.idempotency_key.clone()
                )
            )),
            payload_json: serde_json::json!({
                "project_id": input.project_id.clone(),
                "candidate_id": input.candidate_id.clone(),
                "reason": input.reason.clone(),
                "expected_project_version": input.expected_project_version,
                "principal_id": input.principal_id.clone(),
                "authorization_event_id": input.explicit_event.clone(),
                "authorization_basis": input.authorization_basis.clone(),
            })
            .to_string(),
            created_at: input.updated_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_decision_candidate WHERE id = ?")
            .bind(&input.candidate_id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_decision_candidate(row)
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
                // An approved definition is what makes a milestone active
                // work. Baseline activation used to perform this transition;
                // the approved Charter and its milestone definition are the
                // authority now, so the pointer advance carries it.
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
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
                // An approved definition is what makes a milestone active
                // work. Baseline activation used to perform this transition;
                // the approved Charter and its milestone definition are the
                // authority now, so the pointer advance carries it.
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
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

    async fn create_project_milestone_command(
        &self,
        input: CreateProjectMilestoneCommand,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("project_id", input.milestone.project_id.as_str())],
            )?;
            let milestone_id = command_outcome_string(&receipt, "milestone_id")?;
            let revision_id = command_outcome_string(&receipt, "revision_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT r.*, m.project_id
                 FROM project_milestone_revision r
                 JOIN project_milestone m ON m.id = r.milestone_id
                 WHERE r.id = ? AND r.milestone_id = ?",
            )
            .bind(&revision_id)
            .bind(&milestone_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_id: String = row.try_get("project_id")?;
            validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
            let record = map_milestone_revision(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        if input.milestone.id != input.revision.milestone_id
            || input.revision.expected_milestone_version != 1
            || input.revision.base_revision != 0
            || input.revision.base_revision_id.is_some()
        {
            return Err(DbError::VersionConflict);
        }
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &input.milestone.project_id,
        )?;
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&input.milestone.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.milestone.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        let (milestone_sequence, milestone_key) = if input.allocate_project_sequence {
            let sequence: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(milestone_sequence), 0) + 1
                 FROM project_milestone WHERE project_id = ?",
            )
            .bind(&input.milestone.project_id)
            .fetch_one(&mut *tx)
            .await?;
            (sequence, format!("M{sequence:03}"))
        } else {
            (
                input.milestone.milestone_sequence,
                input.milestone.milestone_key.clone(),
            )
        };
        sqlx::query(
            "INSERT INTO project_milestone (
                id, project_id, milestone_sequence, milestone_key, display_label,
                lifecycle, blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'planned', '[]', '[]', '[]', 1, ?, ?)",
        )
        .bind(&input.milestone.id)
        .bind(&input.milestone.project_id)
        .bind(milestone_sequence)
        .bind(&milestone_key)
        .bind(input.milestone.display_label.as_deref())
        .bind(&input.milestone.created_at)
        .bind(&input.milestone.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let project_updated = sqlx::query(
            "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.milestone.updated_at)
        .bind(&input.milestone.project_id)
        .bind(input.milestone.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if project_updated.rows_affected() != 1 {
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
                rendered_digest, author_type, author_id, source_refs_json, created_at
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
        .map_err(orchestration_write_error)?;
        materialize_milestone_check_definitions_in_tx(
            &mut tx,
            &input.milestone.project_id,
            &input.milestone.id,
            &input.revision,
            &input.check_definitions,
        )
        .await?;
        let milestone_updated = if input.revision.lifecycle == "draft" {
            sqlx::query(
                "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND version = 1",
            )
            .bind(&input.revision.created_at)
            .bind(&input.milestone.id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?
        } else {
            sqlx::query(
                // An approved definition is what makes a milestone active
                // work. Baseline activation used to perform this transition;
                // the approved Charter and its milestone definition are the
                // authority now, so the pointer advance carries it.
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = 1",
            )
            .bind(&input.revision.id)
            .bind(&input.revision.created_at)
            .bind(&input.milestone.id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?
        };
        if milestone_updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                input.revision.author_type.clone(),
                input.revision.author_id.clone(),
                input.revision.id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.definition.created".to_owned(),
            entity_type: "project_milestone_revision".to_owned(),
            entity_id: input.revision.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.milestone.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-milestone-revision-created:{}",
                input.revision.id
            )),
            payload_json: serde_json::json!({
                "operation": "project.milestone",
                "project_id": input.milestone.project_id.clone(),
                "milestone_id": input.milestone.id.clone(),
                "revision_id": input.revision.id.clone(),
                "revision": 1,
                "lifecycle": input.revision.lifecycle.clone(),
                "content_digest": input.revision.content_digest.clone(),
                "rendered_digest": input.revision.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.revision.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
            .bind(&input.revision.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_milestone_revision(row)
    }

    async fn append_project_milestone_revision_command(
        &self,
        input: AppendProjectMilestoneRevisionCommand,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("milestone_id", input.revision.milestone_id.as_str())],
            )?;
            let revision_id = command_outcome_string(&receipt, "revision_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(
                "SELECT r.*, m.project_id
                 FROM project_milestone_revision r
                 JOIN project_milestone m ON m.id = r.milestone_id
                 WHERE r.id = ? AND r.milestone_id = ?",
            )
            .bind(&revision_id)
            .bind(&input.revision.milestone_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            let project_id: String = row.try_get("project_id")?;
            validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
            let record = map_milestone_revision(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        let milestone = sqlx::query(
            "SELECT project_id, version, current_definition_revision_id
             FROM project_milestone WHERE id = ?",
        )
        .bind(&input.revision.milestone_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let project_id: String = milestone.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let milestone_version: i64 = milestone.try_get("version")?;
        if milestone_version != input.revision.expected_milestone_version {
            return Err(DbError::VersionConflict);
        }
        let current_revision_id: Option<String> =
            milestone.try_get("current_definition_revision_id")?;
        if input.revision.base_revision > 0 {
            let Some(base_revision_id) = input.revision.base_revision_id.as_deref() else {
                return Err(DbError::VersionConflict);
            };
            if let Some(current_revision_id) = current_revision_id.as_deref() {
                if current_revision_id != base_revision_id {
                    return Err(DbError::VersionConflict);
                }
            } else {
                // A first draft intentionally leaves the current-definition
                // pointer NULL.  In that state the runtime projects the
                // latest immutable revision as the effective editing base.
                let latest_revision_id: Option<String> = sqlx::query_scalar(
                    "SELECT id FROM project_milestone_revision
                     WHERE milestone_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
                )
                .bind(&input.revision.milestone_id)
                .fetch_optional(&mut *tx)
                .await?;
                if latest_revision_id.as_deref() != Some(base_revision_id) {
                    return Err(DbError::VersionConflict);
                }
            }
            let base_matches: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_milestone_revision
                 WHERE id = ? AND milestone_id = ? AND revision = ? LIMIT 1",
            )
            .bind(base_revision_id)
            .bind(&input.revision.milestone_id)
            .bind(input.revision.base_revision)
            .fetch_optional(&mut *tx)
            .await?;
            if base_matches.is_none() {
                return Err(DbError::VersionConflict);
            }
        } else if input.revision.base_revision_id.is_some() || current_revision_id.is_some() {
            return Err(DbError::VersionConflict);
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_milestone_revision WHERE milestone_id = ?",
        )
        .bind(&input.revision.milestone_id)
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
                rendered_digest, author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.milestone_id)
        .bind(revision)
        .bind(input.revision.base_revision)
        .bind(input.revision.base_revision_id.as_deref())
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
        .map_err(orchestration_write_error)?;
        materialize_milestone_check_definitions_in_tx(
            &mut tx,
            &project_id,
            &input.revision.milestone_id,
            &input.revision,
            &input.check_definitions,
        )
        .await?;
        let advanced = if input.revision.lifecycle == "draft" {
            sqlx::query(
                "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
            )
            .bind(&input.revision.created_at)
            .bind(&input.revision.milestone_id)
            .bind(&project_id)
            .bind(input.revision.expected_milestone_version)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?
        } else {
            sqlx::query(
                // An approved definition is what makes a milestone active
                // work. Baseline activation used to perform this transition;
                // the approved Charter and its milestone definition are the
                // authority now, so the pointer advance carries it.
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
            )
            .bind(&input.revision.id)
            .bind(&input.revision.created_at)
            .bind(&input.revision.milestone_id)
            .bind(&project_id)
            .bind(input.revision.expected_milestone_version)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?
        };
        if advanced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                input.revision.author_type.clone(),
                input.revision.author_id.clone(),
                input.revision.id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.definition.revised".to_owned(),
            entity_type: "project_milestone_revision".to_owned(),
            entity_id: input.revision.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-milestone-revision-created:{}",
                input.revision.id
            )),
            payload_json: serde_json::json!({
                "operation": "project.milestone",
                "milestone_id": input.revision.milestone_id.clone(),
                "revision_id": input.revision.id.clone(),
                "revision": revision,
                "lifecycle": input.revision.lifecycle.clone(),
                "content_digest": input.revision.content_digest.clone(),
                "rendered_digest": input.revision.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.revision.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
            .bind(&input.revision.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_milestone_revision(row)
    }

    async fn set_primary_project_milestone_command(
        &self,
        input: SetPrimaryProjectMilestoneCommand,
    ) -> Result<Project> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, input.command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[("project_id", input.project_id.as_str())],
            )?;
            validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref())
                .await?;
            let row = sqlx::query(&format!(
                "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
            ))
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
            let project = map_project(row)?;
            tx.commit().await?;
            return Ok(project);
        }

        validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
        if input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        let project = sqlx::query("SELECT version FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project.try_get("version")?;
        if project_version != input.expected_project_version {
            return Err(DbError::VersionConflict);
        }
        let active_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_milestone
             WHERE project_id = ? AND lifecycle = 'active'",
        )
        .bind(&input.project_id)
        .fetch_one(&mut *tx)
        .await?;
        if active_count > 0 && input.primary_milestone_id.is_none() {
            return Err(DbError::VersionConflict);
        }
        if let Some(milestone_id) = input.primary_milestone_id.as_deref() {
            let valid: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_milestone
                 WHERE id = ? AND project_id = ? AND lifecycle IN ('planned', 'active')",
            )
            .bind(milestone_id)
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?;
            if valid.is_none() {
                return Err(DbError::VersionConflict);
            }
        }
        let updated = sqlx::query(
            "UPDATE project SET primary_milestone_id = ?, version = version + 1,
                 updated_at = ? WHERE id = ? AND version = ?",
        )
        .bind(input.primary_milestone_id.as_deref())
        .bind(&input.updated_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                input.command_receipt.as_ref(),
                input.principal_type.clone(),
                Some(input.principal_id.clone()),
                input.explicit_event.clone(),
                Some(input.explicit_event.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.primary.set".to_owned(),
            entity_type: "project".to_owned(),
            entity_id: input.project_id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-milestone-primary-set:{}",
                input.idempotency_key
            )),
            payload_json: serde_json::json!({
                "operation": "project.milestone",
                "project_id": input.project_id.clone(),
                "primary_milestone_id": input.primary_milestone_id.clone(),
                "expected_project_version": input.expected_project_version,
                "principal_id": input.principal_id.clone(),
            })
            .to_string(),
            created_at: input.updated_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(
            self,
            &mut tx,
            &event.id,
            input.command_receipt,
            input.action_execution,
        )
        .await?;
        let row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(&input.project_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        map_project(row)
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
        command: AppendProjectMilestoneCheckResultCommand,
    ) -> Result<ProjectMilestoneCheckResultRecord> {
        let AppendProjectMilestoneCheckResultCommand {
            result: input,
            command_receipt,
            action_execution,
        } = command;
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
        validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
        if let Some(receipt) =
            resolve_command_replay(self, &mut tx, command_receipt.as_ref()).await?
        {
            validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
            let result_id = command_outcome_string(&receipt, "result_id")?;
            let row = sqlx::query("SELECT * FROM project_milestone_check_result WHERE id = ?")
                .bind(&result_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::IdempotencyConflict)?;
            tx.commit().await?;
            return map_milestone_result(row);
        }
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
                governing_charter_revision_id,
                principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at, expected_version,
                explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                command_receipt.as_ref(),
                input.principal_type.clone(),
                Some(input.principal_id.clone()),
                input.id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.milestone.check.recorded".to_owned(),
            entity_type: "project_milestone_check_result".to_owned(),
            entity_id: input.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!("milestone-check-recorded:{}", input.id)),
            payload_json: serde_json::json!({
                "project_id": input.project_id.clone(),
                "milestone_id": input.milestone_id.clone(),
                "check_id": input.check_id.clone(),
                "definition_revision_id": input.definition_revision_id.clone(),
                "source_kind": input.source_kind.clone(),
                "outcome": input.outcome.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(self, &mut tx, &event.id, command_receipt, action_execution).await?;
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
        sqlx::query(
            "INSERT INTO project_readiness_snapshot (
                id, project_id, milestone_id, definition_revision_id,
                input_manifest_json, event_watermark, outcome,
                blocking_reasons_json, check_results_json,
                waiver_manifest_json, evidence_manifest_json, commit_context_json,
                computing_policy_revision, readiness_digest, principal_type,
                principal_id, authorization_basis, authorization_action,
                authorization_occurred_at, expected_milestone_version,
                explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(&input.definition_revision_id)
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
                     reconciliation_reason_json = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
            )
            .bind(lifecycle)
            .bind(&input.blocker_projection_json)
            .bind(&input.stale_projection_json)
            .bind(&input.reconciliation_projection_json)
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

    async fn create_project_readiness_snapshot_command(
        &self,
        input: CreateProjectReadinessSnapshotCommand,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        let CreateProjectReadinessSnapshotCommand {
            snapshot: input,
            command_receipt,
            action_execution,
        } = input;
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[
                    ("project_id", input.project_id.as_str()),
                    ("milestone_id", input.milestone_id.as_str()),
                ],
            )?;
            let snapshot_id = command_outcome_string(&receipt, "readiness_snapshot_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
            let row = sqlx::query(
                "SELECT r.* FROM project_readiness_snapshot r
                 WHERE r.id = ? AND r.project_id = ? AND r.milestone_id = ?",
            )
            .bind(&snapshot_id)
            .bind(&input.project_id)
            .bind(&input.milestone_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
            let record = map_readiness(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        if input.principal_type.trim().is_empty()
            || input.principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
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
        validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
        // Readiness candidates are computed in a separate transaction from
        // this command persistence boundary. Re-check the source watermark
        // while holding the command's write lock so a Project mutation that
        // committed after candidate computation cannot be hidden by the
        // readiness snapshot. The readiness event itself is deliberately
        // excluded: it is the command's derived output, not a source input.
        let current_source_watermark: String = sqlx::query_scalar(
            "SELECT COALESCE(
                 (SELECT id FROM domain_event
                  WHERE scope_type = 'project' AND scope_id = ?
                    AND event_type NOT IN (
                        'milestone.readiness.evaluated',
                        'project_release.candidate_requested'
                    )
                  ORDER BY sequence DESC LIMIT 1),
                 'none'
             )",
        )
        .bind(&input.project_id)
        .fetch_one(&mut *tx)
        .await?;
        if current_source_watermark != input.event_watermark {
            return Err(DbError::VersionConflict);
        }
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
        sqlx::query(
            "INSERT INTO project_readiness_snapshot (
                id, project_id, milestone_id, definition_revision_id,
                input_manifest_json, event_watermark, outcome,
                blocking_reasons_json, check_results_json, waiver_manifest_json,
                evidence_manifest_json, commit_context_json, computing_policy_revision,
                readiness_digest, principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at,
                expected_milestone_version, explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .bind(&input.definition_revision_id)
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
        .map_err(orchestration_write_error)?;
        if milestone_lifecycle != "released" {
            let lifecycle = if input.outcome == "ready" {
                "ready_for_release"
            } else {
                "active"
            };
            let updated = sqlx::query(
                "UPDATE project_milestone
                 SET lifecycle = ?, blocker_reason_json = ?, stale_reason_json = ?,
                     reconciliation_reason_json = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
            )
            .bind(lifecycle)
            .bind(&input.blocker_projection_json)
            .bind(&input.stale_projection_json)
            .bind(&input.reconciliation_projection_json)
            .bind(&input.created_at)
            .bind(&input.milestone_id)
            .bind(&input.project_id)
            .bind(input.expected_milestone_version)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if updated.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                command_receipt.as_ref(),
                input.principal_type.clone(),
                Some(input.principal_id.clone()),
                input.id.clone(),
                Some(input.explicit_event.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.readiness.evaluated".to_owned(),
            entity_type: "project_readiness_snapshot".to_owned(),
            entity_id: input.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!("project-readiness-snapshot-created:{}", input.id)),
            payload_json: serde_json::json!({
                "operation": "project.milestone.readiness",
                "project_id": input.project_id.clone(),
                "milestone_id": input.milestone_id.clone(),
                "readiness_snapshot_id": input.id.clone(),
                "readiness_digest": input.readiness_digest.clone(),
                "result": input.outcome.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(self, &mut tx, &event.id, command_receipt, action_execution).await?;
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
                || existing.releasing_principal_display_name
                    != input.releasing_principal_display_name
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
            "SELECT definition_revision_id
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
        if definition_revision_id != input.milestone_revision_id {
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
                readiness_digest, summary, changelog,
                known_issues_json,
                charter_revision_id, document_revisions_json, decision_ids_json,
                task_references_json, validation_references_json, git_references_json,
                evidence_references_json, waivers_json, releasing_principal_type,
                releasing_principal_id, releasing_principal_display_name,
                authorization_basis, authorization_action,
                authorization_occurred_at, explicit_event, schema_version,
                snapshot_digest, idempotency_key, created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
        .bind(input.releasing_principal_display_name.as_deref())
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

    async fn create_project_release_command(
        &self,
        input: CreateProjectReleaseCommand,
    ) -> Result<ProjectReleaseRecord> {
        let CreateProjectReleaseCommand {
            release: input,
            references,
            command_receipt,
            action_execution,
        } = input;
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[
                    ("project_id", input.project_id.as_str()),
                    ("milestone_id", input.milestone_id.as_str()),
                ],
            )?;
            let release_id = command_outcome_string(&receipt, "release_id")?;
            validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
            let row = sqlx::query(
                "SELECT * FROM project_release
                 WHERE id = ? AND project_id = ? AND milestone_id = ?",
            )
            .bind(&release_id)
            .bind(&input.project_id)
            .bind(&input.milestone_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
            let record = map_release(row)?;
            tx.commit().await?;
            return Ok(record);
        }

        if input.releasing_principal_type.trim().is_empty()
            || input.releasing_principal_id.trim().is_empty()
            || input.authorization_basis.trim().is_empty()
            || input.authorization_action.trim().is_empty()
            || input.explicit_event.trim().is_empty()
            || !valid_authorization_timestamp(&input.authorization_occurred_at)
        {
            return Err(DbError::VersionConflict);
        }
        validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
        if references
            .iter()
            .any(|reference| reference.release_id != input.id)
        {
            return Err(DbError::VersionConflict);
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
        let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
        if milestone_lifecycle != "ready_for_release" {
            return Err(DbError::VersionConflict);
        }
        let readiness = sqlx::query(
            "SELECT definition_revision_id
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
        if definition_revision_id != input.milestone_revision_id {
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
                readiness_digest, summary, changelog,
                known_issues_json, charter_revision_id, document_revisions_json,
                decision_ids_json, task_references_json, validation_references_json,
                git_references_json, evidence_references_json, waivers_json,
                releasing_principal_type, releasing_principal_id,
                releasing_principal_display_name, authorization_basis,
                authorization_action, authorization_occurred_at, explicit_event,
                schema_version, snapshot_digest, idempotency_key, created_at
             ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
        .bind(input.releasing_principal_display_name.as_deref())
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
        .map_err(orchestration_write_error)?;
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
            .map_err(orchestration_write_error)?;
        }
        let released = sqlx::query(
            "UPDATE project_milestone SET lifecycle = 'released', version = version + 1,
                 updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle = 'ready_for_release'",
        )
        .bind(&input.created_at)
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .bind(input.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if released.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                command_receipt.as_ref(),
                input.releasing_principal_type.clone(),
                Some(input.releasing_principal_id.clone()),
                input.id.clone(),
                Some(input.explicit_event.clone()),
                0,
            );
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.release.created".to_owned(),
            entity_type: "project_release".to_owned(),
            entity_id: input.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!("project-release-created:{}", input.id)),
            payload_json: serde_json::json!({
                "operation": "project.release",
                "project_id": input.project_id.clone(),
                "milestone_id": input.milestone_id.clone(),
                "release_id": input.id.clone(),
                "release_identifier": input.release_identifier.clone(),
                "release_revision": input.release_revision,
                "readiness_snapshot_id": input.readiness_snapshot_id.clone(),
                "readiness_digest": input.readiness_digest.clone(),
                "snapshot_digest": input.snapshot_digest.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(self, &mut tx, &event.id, command_receipt, action_execution).await?;
        let row = sqlx::query("SELECT * FROM project_release WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        map_release(row)
    }

    async fn create_project_release_request_command(
        &self,
        input: CreateProjectReleaseRequestCommand,
    ) -> Result<ProjectReleaseRequestRecord> {
        let CreateProjectReleaseRequestCommand {
            request,
            command_receipt,
            action_execution,
        } = input;
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let replay = resolve_command_replay(self, &mut tx, command_receipt.as_ref()).await?;
        if let Some(receipt) = replay {
            validate_command_outcome_identity(
                &receipt,
                &[
                    ("project_id", request.project_id.as_str()),
                    ("milestone_id", request.milestone_id.as_str()),
                    (
                        "readiness_snapshot_id",
                        request.readiness_snapshot_id.as_str(),
                    ),
                ],
            )?;
            validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
            let event_id = serde_json::from_str::<serde_json::Value>(&receipt.outcome_json)
                .ok()
                .and_then(|outcome| {
                    outcome
                        .get("candidate_event_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| receipt.event_id.clone());
            let event = sqlx::query(
                "SELECT * FROM domain_event
                 WHERE id = ? AND event_type = 'project_release.candidate_requested'
                   AND scope_type = 'project' AND scope_id = ?",
            )
            .bind(&event_id)
            .bind(&request.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
            validate_command_scope(command_receipt.as_ref(), "project", &request.project_id)?;
            let payload: serde_json::Value =
                serde_json::from_str(&event.try_get::<String, _>("payload_json")?)
                    .map_err(|_| DbError::IdempotencyConflict)?;
            let record = ProjectReleaseRequestRecord {
                event_id,
                project_id: payload
                    .get("project_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(DbError::IdempotencyConflict)?
                    .to_owned(),
                milestone_id: payload
                    .get("milestone_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(DbError::IdempotencyConflict)?
                    .to_owned(),
                expected_milestone_version: payload
                    .get("milestone_version")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or(DbError::IdempotencyConflict)?,
                readiness_snapshot_id: payload
                    .get("readiness_snapshot_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(DbError::IdempotencyConflict)?
                    .to_owned(),
                readiness_digest: payload
                    .get("readiness_digest")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(DbError::IdempotencyConflict)?
                    .to_owned(),
                status: payload
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(DbError::IdempotencyConflict)?
                    .to_owned(),
                idempotency_key: request.idempotency_key.clone(),
                created_at: event.try_get("created_at")?,
            };
            tx.commit().await?;
            return Ok(record);
        }

        validate_command_scope(command_receipt.as_ref(), "project", &request.project_id)?;
        if request.event_id.trim().is_empty()
            || request.project_id.trim().is_empty()
            || request.milestone_id.trim().is_empty()
            || request.readiness_snapshot_id.trim().is_empty()
            || request.readiness_digest.trim().is_empty()
            || request.status.trim().is_empty()
            || request.idempotency_key.trim().is_empty()
        {
            return Err(DbError::Check(
                "release request command input is incomplete".to_owned(),
            ));
        }
        let milestone = sqlx::query(
            "SELECT version, lifecycle FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(&request.milestone_id)
        .bind(&request.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let milestone_version: i64 = milestone.try_get("version")?;
        let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
        if milestone_version != request.expected_milestone_version
            || milestone_lifecycle != "ready_for_release"
        {
            return Err(DbError::VersionConflict);
        }
        let ready: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND readiness_digest = ? AND outcome = 'ready' LIMIT 1",
        )
        .bind(&request.readiness_snapshot_id)
        .bind(&request.project_id)
        .bind(&request.milestone_id)
        .bind(&request.readiness_digest)
        .fetch_optional(&mut *tx)
        .await?;
        if ready.is_none() {
            return Err(DbError::VersionConflict);
        }
        let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
            command_event_provenance(
                command_receipt.as_ref(),
                "agent".to_owned(),
                None,
                request.event_id.clone(),
                None,
                0,
            );
        let event = CreateDomainEvent {
            id: request.event_id.clone(),
            event_type: "project_release.candidate_requested".to_owned(),
            entity_type: "project_readiness_snapshot".to_owned(),
            entity_id: request.readiness_snapshot_id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: request.project_id.clone(),
            correlation_id,
            causation_id,
            causation_depth,
            dedupe_key: Some(format!(
                "project-release-request:{}",
                request.idempotency_key
            )),
            payload_json: serde_json::json!({
                "operation": "project.release",
                "project_id": request.project_id.clone(),
                "milestone_id": request.milestone_id.clone(),
                "milestone_version": request.expected_milestone_version,
                "readiness_snapshot_id": request.readiness_snapshot_id.clone(),
                "readiness_digest": request.readiness_digest.clone(),
                "status": request.status.clone(),
                "final_release_created": false,
            })
            .to_string(),
            created_at: request.created_at.clone(),
        };
        let event = DomainEventRepo::append_event_in_tx(self, &mut tx, &event).await?;
        finalize_command_in_tx(self, &mut tx, &event.id, command_receipt, action_execution).await?;
        let payload: serde_json::Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| DbError::Check("release request event payload is invalid".to_owned()))?;
        let record = ProjectReleaseRequestRecord {
            event_id: event.id.clone(),
            project_id: request.project_id,
            milestone_id: request.milestone_id,
            expected_milestone_version: request.expected_milestone_version,
            readiness_snapshot_id: request.readiness_snapshot_id,
            readiness_digest: request.readiness_digest,
            status: payload
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(request.status.as_str())
                .to_owned(),
            idempotency_key: request.idempotency_key,
            created_at: event.created_at.clone(),
        };
        tx.commit().await?;
        Ok(record)
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

        // Main supplies the canonical command identity with the composite
        // request. Resolve it while this transaction is still open so a
        // response-loss replay can validate the durable receipt before
        // entering the consumed approval path. `get_command_receipt_in_tx`
        // deliberately turns a changed principal or digest for the same
        // canonical key into IdempotencyConflict.
        let replayed_command_receipt = if let Some(receipt) = input.command_receipt.as_ref() {
            CommandReceiptRepo::get_command_receipt_in_tx(
                self,
                &mut tx,
                &receipt.principal_type,
                &receipt.principal_id,
                &receipt.scope_type,
                &receipt.scope_id,
                &receipt.operation,
                &receipt.idempotency_key,
                &receipt.input_digest,
            )
            .await?
        } else {
            None
        };

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
            let replay_ids = if let Some(receipt) = replayed_command_receipt.as_ref() {
                let project_id = command_outcome_string(receipt, "project_id")?;
                let binding_id = command_outcome_string(receipt, "project_agent_binding_id")?;
                let project_chat_id = command_outcome_string(receipt, "project_chat_id")?;
                let handoff_id = command_outcome_string(receipt, "handoff_id")?;
                let target_message_id = command_outcome_string(receipt, "target_message_id")?;
                let target_turn_id = command_outcome_string(receipt, "target_turn_id")?;
                validate_command_outcome_identity(
                    receipt,
                    &[
                        ("charter_id", approval.charter_id.as_str()),
                        ("charter_revision_id", approval.revision_id.as_str()),
                    ],
                )?;
                let valid_scope = (receipt.scope_type == "account"
                    && receipt.scope_id == input.account_id)
                    || (receipt.scope_type == "agent_chat" && receipt.scope_id == main_chat_id);
                if !valid_scope {
                    return Err(DbError::IdempotencyConflict);
                }
                Some((
                    project_id,
                    binding_id,
                    project_chat_id,
                    handoff_id,
                    target_message_id,
                    target_turn_id,
                ))
            } else {
                None
            };
            let project_id = replay_ids
                .as_ref()
                .map(|ids| ids.0.as_str())
                .or(approval.consumed_project_id.as_deref())
                .ok_or(DbError::NotFound)?;
            if replay_ids
                .as_ref()
                .is_some_and(|ids| approval.consumed_project_id.as_deref() != Some(ids.0.as_str()))
            {
                return Err(DbError::IdempotencyConflict);
            }
            let project_row = sqlx::query(&format!(
                "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
            ))
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(DbError::from)?;
            let project = map_project(project_row)?;
            if (replayed_command_receipt.is_none() && project.id != input.project.id)
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
                   AND b.state IN ('active', 'replaced')
                 WHERE c.project_id = ? AND c.kind = 'project'
                   AND (? IS NULL OR c.id = ?) AND b.id = ?
                 LIMIT 1",
            )
            .bind(project_id)
            .bind(replay_ids.as_ref().map(|ids| ids.2.as_str()))
            .bind(replay_ids.as_ref().map(|ids| ids.2.as_str()))
            .bind(
                replay_ids
                    .as_ref()
                    .map(|ids| ids.1.as_str())
                    .unwrap_or(input.project_agent_binding_id.as_str()),
            )
            .fetch_one(&mut *tx)
            .await?;
            if replay_ids
                .as_ref()
                .is_some_and(|ids| project_chat_id != ids.2 || binding_id != ids.1)
                || chat_status != "ready"
                || !matches!(binding_state.as_str(), "active" | "replaced")
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
                 WHERE id = ? AND target_chat_id = ? AND dedupe_key = ? LIMIT 1",
            )
            .bind(
                replay_ids
                    .as_ref()
                    .map(|ids| ids.3.as_str())
                    .unwrap_or(input.handoff_id.as_str()),
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
            if replay_ids.as_ref().is_some_and(|ids| handoff_id != ids.3)
                || handoff_id != input.handoff_id && replayed_command_receipt.is_none()
                || source_chat_id != main_chat_id
                || stored_source_message_id != input.source_message_id
                || stored_source_turn_id != input.source_turn_id
                || replay_ids
                    .as_ref()
                    .map(|ids| target_message_id != ids.4 || target_turn_id != ids.5)
                    .unwrap_or(
                        target_message_id != input.target_message_id
                            || target_turn_id != input.target_turn_id,
                    )
                || stored_author_identity_id.as_deref()
                    != Some(
                        input
                            .source_identity_id
                            .as_deref()
                            .ok_or(DbError::VersionConflict)?,
                )
                || stored_content != input.handoff_content
                || stored_content_guard != input.content_guard_json
                // A contender can enter this transaction before the winning
                // receipt is visible and therefore carry a different
                // server-minted transport correlation.  Once the receipt is
                // resolved, the committed packet is authoritative; only a
                // fresh mutation may be compared with the candidate value.
                || (replayed_command_receipt.is_none()
                    && stored_correlation_id != input.correlation_id)
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
            if json_string(&stored_source_value, "correlation_id")
                != Some(stored_correlation_id.as_str())
                || json_string(stored_source, "identity_id") != input.source_identity_id.as_deref()
                || json_string(stored_source, "profile_revision_id")
                    != input.source_profile_id.as_deref()
                || json_string(stored_source, "instruction_revision_id")
                    != input.source_instruction_revision_id.as_deref()
                || json_string(stored_source, "message_id") != input.source_message_id.as_deref()
                || json_string(stored_source, "turn_id") != input.source_turn_id.as_deref()
                || json_string(stored_project, "id") != Some(project.id.as_str())
                || json_string(stored_project, "name") != Some(input.project.name.as_str())
                || json_string(stored_project, "mode")
                    != Some(approval.approved_project_mode.as_str())
                || json_string(stored_target, "chat_id") != Some(project_chat_id.as_str())
                || json_string(stored_target, "binding_id") != Some(binding_id.as_str())
                || json_string(stored_target, "message_id") != Some(target_message_id.as_str())
                || json_string(stored_target, "turn_id") != Some(target_turn_id.as_str())
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

            let replayed = CreatedProjectFromCharterApproval {
                project,
                project_agent_binding_id: binding_id,
                project_chat_id,
                charter_id: approval.charter_id,
                charter_revision_id: approval.revision_id,
                handoff_id,
                target_message_id,
                target_turn_id,
            };
            if let Some(receipt) = replayed_command_receipt.as_ref() {
                let expected_outcome = serde_json::json!({
                    "operation": receipt.operation,
                    "project_id": replayed.project.id,
                    "project_agent_binding_id": replayed.project_agent_binding_id,
                    "project_chat_id": replayed.project_chat_id,
                    "charter_id": replayed.charter_id,
                    "charter_revision_id": replayed.charter_revision_id,
                    "handoff_id": replayed.handoff_id,
                    "target_message_id": replayed.target_message_id,
                    "target_turn_id": replayed.target_turn_id,
                });
                let stored_outcome =
                    serde_json::from_str::<serde_json::Value>(&receipt.outcome_json)
                        .map_err(|_| DbError::IdempotencyConflict)?;
                if stored_outcome != expected_outcome {
                    return Err(DbError::IdempotencyConflict);
                }
                // The receipt's execution is authoritative.  A response-loss
                // retry may carry a fresh execution shell (including the
                // pre-finalization placeholder outcome), so validate the
                // persisted provenance/linkage without treating that shell's
                // server-minted fields as caller identity.
                validate_replay_action_bundle(&mut tx, receipt, None).await?;
                let event = sqlx::query(
                    "SELECT event_type, entity_type, entity_id, scope_type, scope_id,
                            payload_json
                     FROM domain_event WHERE id = ?",
                )
                .bind(&receipt.event_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(DbError::IdempotencyConflict)?;
                let event_type: String = event.try_get("event_type")?;
                let entity_type: String = event.try_get("entity_type")?;
                let entity_id: String = event.try_get("entity_id")?;
                let event_scope_type: String = event.try_get("scope_type")?;
                let event_scope_id: String = event.try_get("scope_id")?;
                if event_type != "project.created_from_charter_approval"
                    || entity_type != "project"
                    || entity_id != replayed.project.id
                    || event_scope_type != "project"
                    || event_scope_id != replayed.project.id
                {
                    return Err(DbError::IdempotencyConflict);
                }
                let payload: serde_json::Value =
                    serde_json::from_str(&event.try_get::<String, _>("payload_json")?)
                        .map_err(|_| DbError::IdempotencyConflict)?;
                if json_string(&payload, "project_id") != Some(replayed.project.id.as_str())
                    || json_string(&payload, "charter_id") != Some(replayed.charter_id.as_str())
                    || json_string(&payload, "charter_revision_id")
                        != Some(replayed.charter_revision_id.as_str())
                    || json_string(&payload, "approval_id") != Some(approval.id.as_str())
                    || json_string(&payload, "handoff_id") != Some(replayed.handoff_id.as_str())
                    || json_string(&payload, "project_chat_id")
                        != Some(replayed.project_chat_id.as_str())
                    || json_string(&payload, "project_agent_binding_id")
                        != Some(replayed.project_agent_binding_id.as_str())
                {
                    return Err(DbError::IdempotencyConflict);
                }
            }
            tx.commit().await?;
            return Ok(replayed);
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
            // A frozen Genesis source turn must still authenticate the
            // historical Main binding/Profile snapshot while this composite
            // holds its IMMEDIATE transaction lock.  Legacy pre-V088 source
            // turns take the conservative no-provenance path in the shared
            // validator; they are never silently rebound to a new identity.
            super::agent_chat::validate_agent_chat_turn_job_id(&mut tx, source_turn_id).await?;
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

        // The Project-create commit also schedules the durable provisioning
        // operation and its complete checkpoint set.  This is intentionally
        // before any response/follow-up work: a process stop after this
        // transaction commits still leaves a recoverable operation for the
        // next replay or background reconciler.
        sqlx::query(
            "INSERT INTO project_provisioning_operation (
                id, project_id, idempotency_key, status, current_checkpoint,
                attempt_count, max_attempts, retryable,
                last_error_code, last_error_message, created_at, updated_at,
                version
             ) VALUES (?, ?, ?, 'setup_required', 'preflight', 0, ?, 1,
                       NULL, NULL, ?, ?, 1)",
        )
        .bind(&input.provisioning_operation_id)
        .bind(&input.project.id)
        .bind(format!("project-provisioning:{}", input.project.id))
        .bind(input.max_attempts)
        .bind(&input.project.created_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        for checkpoint in [
            "preflight",
            "repository_initialized",
            "repository_registered",
            "repository_linked",
            "roles_assigned",
        ] {
            sqlx::query(
                "INSERT INTO project_provisioning_checkpoint (
                    id, operation_id, checkpoint, status, attempt_count,
                    error_code, error_message, details_json, started_at,
                    completed_at, created_at, updated_at, version
                 ) VALUES (?, ?, ?, 'pending', 0, NULL, NULL, '{}', NULL,
                           NULL, ?, ?, 1)",
            )
            .bind(new_uuid_v4())
            .bind(&input.provisioning_operation_id)
            .bind(checkpoint)
            .bind(&input.project.created_at)
            .bind(&input.project.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
        }

        let (project_chat_id, setup_binding_id): (String, String) = sqlx::query_as(
            "SELECT c.id, b.id FROM agent_chat c
             JOIN project_agent_binding b ON b.project_id = c.project_id
               AND b.state = 'agent_setup_required'
             WHERE c.project_id = ? AND c.kind = 'project'",
        )
        .bind(&input.project.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| {
            DbError::Check(format!("Project setup binding is unavailable: {error}"))
        })?;

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
                       ?, ?, ?, ?, ?, 1, ?, ?)",
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

        // The target Project binding/chat now exist inside this same
        // transaction. Resolve the identity's current Profile and exact
        // policy/skill versions before admitting the handoff turn; the
        // approval's bind-time profile id is not treated as a Profile
        // snapshot. These reads are protected by the IMMEDIATE transaction
        // and are also checked against the values used to create the binding.
        let target_binding = sqlx::query(
            "SELECT version, identity_id, profile_id, operating_skill_revision_id,
                    policy_revision, policy_digest, permission_ceiling_json, state
             FROM project_agent_binding
             WHERE id = ? AND project_id = ?",
        )
        .bind(&input.project_agent_binding_id)
        .bind(&input.project.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| {
            DbError::Check(format!("Project target binding is unavailable: {error}"))
        })?;
        let target_binding_version: i64 = target_binding.try_get("version")?;
        let target_binding_identity_id: Option<String> = target_binding.try_get("identity_id")?;
        let target_binding_profile_id: Option<String> = target_binding.try_get("profile_id")?;
        let target_binding_skill_revision: Option<String> =
            target_binding.try_get("operating_skill_revision_id")?;
        let target_binding_policy_revision: String = target_binding.try_get("policy_revision")?;
        let target_binding_policy_digest: String = target_binding.try_get("policy_digest")?;
        let target_permission_json: String = target_binding.try_get("permission_ceiling_json")?;
        let target_binding_state: String = target_binding.try_get("state")?;
        if target_binding_state != "active"
            || target_binding_identity_id.as_deref() != Some(identity_id.as_str())
            || target_binding_profile_id.as_deref() != Some(profile_id.as_str())
            || target_binding_skill_revision.as_deref() != Some(skill_revision_id.as_str())
            || target_binding_policy_revision != input.policy_revision
            || target_binding_policy_digest != input.policy_digest
        {
            return Err(DbError::VersionConflict);
        }
        let target_identity = sqlx::query(
            "SELECT version, selected_profile_id, paused
             FROM agent_identity WHERE id = ?",
        )
        .bind(&identity_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| {
            DbError::Check(format!("Project target identity is unavailable: {error}"))
        })?;
        let target_identity_version: i64 = target_identity.try_get("version")?;
        let target_selected_profile_id: Option<String> =
            target_identity.try_get("selected_profile_id")?;
        let target_paused: i64 = target_identity.try_get("paused")?;
        if target_paused != 0 || target_selected_profile_id.as_deref() != Some(profile_id.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        let target_profile = sqlx::query(
            "SELECT identity_id, version, tool_policy_json
             FROM agent_profile WHERE id = ?",
        )
        .bind(&profile_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| {
            DbError::Check(format!("Project target Profile is unavailable: {error}"))
        })?;
        let target_profile_identity_id: String = target_profile.try_get("identity_id")?;
        let target_profile_version: i64 = target_profile.try_get("version")?;
        let target_tool_policy_json: String = target_profile.try_get("tool_policy_json")?;
        if target_profile_identity_id != identity_id {
            return Err(DbError::VersionConflict);
        }
        let target_permission_policy_digest =
            super::agent_chat::admission_policy_digest(&target_permission_json)?;
        let target_tool_policy_digest =
            super::agent_chat::admission_policy_digest(&target_tool_policy_json)?;
        let target_provenance = serde_json::json!({
            "chat_id": project_chat_id,
            "canonical_scope_type": "agent_chat",
            "canonical_scope_id": project_chat_id,
            "readiness": "ready",
            "binding_id": input.project_agent_binding_id,
            "binding_version": target_binding_version,
            "identity_id": identity_id,
            "identity_version": target_identity_version,
            "profile_id": profile_id,
            "profile_version": target_profile_version,
            "operating_skill_revision": skill_revision_id,
            "policy_revision": input.policy_revision,
            "policy_digest": input.policy_digest,
            "permission_policy_digest": target_permission_policy_digest,
            "tool_policy_digest": target_tool_policy_digest,
        });
        let source_provenance = if let Some(source_turn_id) = input.source_turn_id.as_deref() {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT canonical_scope_provenance_json
                 FROM agent_chat_turn_job WHERE id = ?",
            )
            .bind(source_turn_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| DbError::Check(format!("Main source turn is unavailable: {error}")))?
            .map(|json| {
                serde_json::from_str::<serde_json::Value>(&json)
                    .map_err(|_| DbError::VersionConflict)
            })
            .transpose()?
        } else {
            None
        };
        let admission_source_value =
            serde_json::from_str::<serde_json::Value>(&input.source_revisions_json).map_err(
                |_| DbError::Check("handoff source_revisions_json must be valid JSON".to_owned()),
            )?;
        let admission_source_revisions = normalize_handoff_request(&admission_source_value)?;
        let admission_source_revisions_json = serde_json::to_string(
            &canonicalize_project_handoff_json(&admission_source_revisions),
        )
        .map_err(|error| DbError::Check(format!("invalid handoff request: {error}")))?;
        let handoff_content_digest = super::agent_chat::handoff_content_digest_for_admission(
            &input.handoff_content,
            &admission_source_revisions_json,
            input.source_message_id.as_deref(),
            input.source_turn_id.as_deref(),
        )?;
        let admission_digest = super::agent_chat::handoff_admission_digest_for_provenance(
            &format!("handoff:{}", input.idempotency_key),
            &handoff_content_digest,
            input.causation_depth.saturating_add(1),
            &target_provenance,
            source_provenance.as_ref(),
        )?;
        let target_provenance_json = serde_json::to_string(&target_provenance)
            .map_err(|_| DbError::Check("target turn provenance is invalid".to_owned()))?;

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

        // Compact Projects start with one explicit M001. Acceptance
        // statements already approved in the Charter become manual checks
        // with required, check-linked evidence; the Project Agent may refine
        // the immutable definition later. Approving the Charter approves the
        // work, so the milestone becomes active with its approved definition
        // rather than waiting on a second artifact.
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
            let mut evidence_requirements = Vec::new();
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
                evidence_requirements.push(serde_json::json!({
                    "id": check_id.clone(),
                    "description": format!("Authoritative evidence for: {description}"),
                    "required": true,
                    "evidence_kind": null,
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
                "evidence_requirements": evidence_requirements.clone(),
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
                           '[]', '[]', ?, ?, '[]', 'Genesis baseline',
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
            .bind(serde_json::to_string(&evidence_requirements).map_err(|error| {
                DbError::Check(format!("invalid compact milestone evidence: {error}"))
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
                     ) VALUES (?, ?, ?, ?, ?, ?, 1, 'manual', 'passed', 1, 1, NULL, ?, ?)",
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
                // An approved definition is what makes a milestone active
                // work. Baseline activation used to perform this transition;
                // the approved Charter and its milestone definition are the
                // authority now, so the pointer advance carries it.
                "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
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
        .await
        .map_err(|error| {
            DbError::Check(format!("Project Chat sequence is unavailable: {error}"))
        })?;
        let source_value = serde_json::from_str::<serde_json::Value>(&input.source_revisions_json)
            .map_err(|_| {
                DbError::Check("handoff source_revisions_json must be valid JSON".to_owned())
            })?;
        if !source_value.is_object() {
            return Err(DbError::Check(
                "handoff source_revisions_json must be a JSON object".to_owned(),
            ));
        }
        let handoff_payload_digest = handoff_request_fingerprint(&source_value, &input)?;
        let source_revisions_json: String = sqlx::query_scalar(
            "SELECT json_set(
                ?,
                '$.handoff_id', ?,
                '$.correlation_id', ?,
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
        .bind(&input.correlation_id)
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
        .bind(&handoff_payload_digest)
        .bind(&input.source_revisions_json)
        .bind(&input.create_principal_type)
        .bind(&input.create_principal_id)
        .bind(&input.create_authorization_basis)
        .bind(&input.create_action)
        .bind(&input.create_event_id)
        .bind(&input.create_occurred_at)
        .bind(&input.project.updated_at)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| {
            DbError::Check(format!("Project handoff packet cannot be frozen: {error}"))
        })?;
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
                responder_binding_id, responder_binding_version, responder_identity_version,
                profile_version, operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json,
                canonical_scope_type, canonical_scope_id, status, dedupe_key,
                max_attempts, correlation_id, causation_id, causation_depth,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'agent_chat', ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.target_turn_id)
        .bind(&project_chat_id)
        .bind(&input.target_message_id)
        .bind(&identity_id)
        .bind(&profile_id)
        .bind(&input.project_agent_binding_id)
        .bind(target_binding_version)
        .bind(target_identity_version)
        .bind(target_profile_version)
        .bind(&skill_revision_id)
        .bind(&input.policy_revision)
        .bind(&input.policy_digest)
        .bind(&target_permission_policy_digest)
        .bind(&target_tool_policy_digest)
        .bind(&admission_digest)
        .bind(&target_provenance_json)
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
            actor_type: input
                .command_receipt
                .as_ref()
                .map(|receipt| receipt.principal_type.clone())
                .unwrap_or_else(|| input.create_principal_type.clone()),
            actor_id: input
                .command_receipt
                .as_ref()
                .map(|receipt| receipt.principal_id.clone())
                .or_else(|| Some(input.create_principal_id.clone())),
            scope_type: "project".to_owned(),
            scope_id: input.project.id.clone(),
            correlation_id: input
                .command_receipt
                .as_ref()
                .map(|receipt| receipt.correlation_id.clone())
                .unwrap_or_else(|| input.correlation_id.clone()),
            causation_id: input
                .command_receipt
                .as_ref()
                .map(|receipt| receipt.causation_id.clone())
                .unwrap_or_else(|| input.causation_id.clone()),
            causation_depth: input
                .command_receipt
                .as_ref()
                .map_or(input.causation_depth, |receipt| receipt.causation_depth),
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

        // Main's exact Project-create command uses this same transaction for
        // its action execution and frozen receipt.  REST retries leave both
        // options unset and retain the historical composite behavior.
        let mut command_receipt = input.command_receipt.clone();
        let mut action_execution = input.action_execution.clone();
        if let Some(receipt) = command_receipt.as_mut() {
            // The DB transaction is the first point at which the generated
            // Project Chat id is authoritative. Rebuild the complete frozen
            // result here, rather than patching a transport placeholder.
            receipt.outcome_json = serde_json::json!({
                "operation": receipt.operation,
                "project_id": input.project.id,
                "project_agent_binding_id": input.project_agent_binding_id,
                "project_chat_id": project_chat_id,
                "charter_id": approval.charter_id,
                "charter_revision_id": approval.revision_id,
                "handoff_id": input.handoff_id,
                "target_message_id": input.target_message_id,
                "target_turn_id": input.target_turn_id,
            })
            .to_string();
            if let Some(execution) = action_execution.as_mut() {
                execution.result_json = Some(receipt.outcome_json.clone());
                execution.action_outcome_json = Some(receipt.outcome_json.clone());
            }
        }
        finalize_command_in_tx(self, &mut tx, &event.id, command_receipt, action_execution).await?;

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

        let admission_receipt_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO project_admission_receipt (
                id, project_id, source_kind, handoff_id,
                initial_charter_approval_id, initial_charter_id,
                initial_charter_revision_id, payload_digest,
                validation_schema_version, validated_at, created_at
             ) VALUES (?, ?, 'genesis_handoff', ?, ?, ?, ?, ?,
                       'forge.project-admission/v1', ?, ?)",
        )
        .bind(&admission_receipt_id)
        .bind(&input.project.id)
        .bind(&input.handoff_id)
        .bind(&approval.id)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(&handoff_payload_digest)
        .bind(&input.project.updated_at)
        .bind(&input.project.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let completed_binding = sqlx::query(
            "UPDATE project_agent_binding
             SET admission_receipt_id = ?, charter_approval_id = ?,
                 charter_setup_required = 0
             WHERE id = ? AND project_id = ? AND state = 'active'
               AND charter_setup_required = 1",
        )
        .bind(&admission_receipt_id)
        .bind(&approval.id)
        .bind(&input.project_agent_binding_id)
        .bind(&input.project.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if completed_binding.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

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
        let replacement_required = matches!(input.action.as_str(), "revised" | "superseded");
        let replacement_present = input.replacement_ref_type.is_some()
            || input.replacement_ref_id.is_some()
            || input.replacement_ref_revision.is_some();
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
            || (replacement_required
                && (input
                    .replacement_ref_type
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .is_empty()
                    || input
                        .replacement_ref_id
                        .as_deref()
                        .unwrap_or_default()
                        .trim()
                        .is_empty()))
            || (!replacement_required && replacement_present)
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
                        authorization_occurred_at, reason, occurred_at,
                        replacement_ref_type, replacement_ref_id, replacement_ref_revision
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
                && resolution.try_get::<String, _>("occurred_at")? == input.occurred_at
                && resolution.try_get::<Option<String>, _>("replacement_ref_type")?
                    == input.replacement_ref_type
                && resolution.try_get::<Option<String>, _>("replacement_ref_id")?
                    == input.replacement_ref_id
                && resolution.try_get::<Option<String>, _>("replacement_ref_revision")?
                    == input.replacement_ref_revision;
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
                replacement_ref_type, replacement_ref_id, replacement_ref_revision,
                created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .bind(&input.replacement_ref_type)
        .bind(&input.replacement_ref_id)
        .bind(&input.replacement_ref_revision)
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
        let event = self
            .append_event_in_tx(&mut tx, &input.domain_event)
            .await?;
        finalize_command_in_tx(self, &mut tx, &event.id, Some(input.command_receipt), None).await?;
        let row = sqlx::query("SELECT * FROM project_reconciliation_record WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *tx)
            .await?;
        let record = map_reconciliation(row)?;
        tx.commit().await?;
        Ok(record)
    }

    async fn get_project_reconciliation_resolution(
        &self,
        id: &str,
    ) -> Result<Option<ProjectReconciliationResolutionRecord>> {
        select_one(
            "SELECT * FROM project_reconciliation_resolution WHERE id = ?",
            self.pool(),
            id,
            map_reconciliation_resolution,
        )
        .await
    }
}
