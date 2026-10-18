use super::command_finalization::{action_scope_resolves_to_command_scope, finalize_command_in_tx};
use super::*;
use crate::*;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteRow, Row};

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

fn required_string(row: &SqliteRow, column: &str) -> Result<String> {
    row.try_get(column).map_err(DbError::from)
}

fn optional_string(row: &SqliteRow, column: &str) -> Result<Option<String>> {
    row.try_get(column).map_err(DbError::from)
}

fn json_string<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(serde_json::Value::as_str)
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

/// A fresh command of a coordination root's lead (principal `task_lead`):
/// the execution it names as its correlation is still running for this
/// identity in the lead's role ON the command's source root, and the root's
/// own row of that role still names this identity. Both names come from the
/// service (`TaskService::lead_adaptive_command`), never from a tool payload;
/// they are checked here against the rows, so a lead removed, replaced or
/// stopped between the service's boundary check and this transaction writes
/// nothing, and neither does a run that is not the root's planning run.
async fn reauthorize_task_lead_command_in_tx(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    receipt: &CreateCommandReceipt,
) -> Result<()> {
    let leads: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM execution e
         JOIN task root ON root.id = e.task_id
              AND root.parent_task_id IS NULL AND root.deleted_at IS NULL
         JOIN task_role_assignment a ON a.task_id = root.id AND a.role_name = e.role
              AND a.assignee_type = 'agent' AND a.assignee_id = e.agent_id
         WHERE e.id = ? AND e.agent_id = ? AND e.status = 'running' AND e.role = ?
           AND root.project_id = ?
           AND root.id = json_extract(?, '$.source_task_id')",
    )
    .bind(&receipt.correlation_id)
    .bind(&receipt.principal_id)
    .bind(crate::inherited_subtask::STEP_CHECK_ROLE)
    .bind(&receipt.scope_id)
    .bind(&receipt.outcome_json)
    .fetch_one(&mut **transaction)
    .await?;
    if receipt.scope_type != "project" || receipt.operation != "task.adaptive" || leads == 0 {
        return Err(DbError::Check(
            "the command's principal does not lead this coordination root".to_owned(),
        ));
    }
    Ok(())
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
    // A root's lead is authorized by its running execution, not by a Project
    // binding: repeat that, and only that, under the writer transaction.
    if receipt.principal_type == crate::inherited_subtask::TASK_LEAD_PRINCIPAL {
        return reauthorize_task_lead_command_in_tx(transaction, receipt).await;
    }
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
    operation_registry::authority::permission_set(value).contains(permission)
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

pub(crate) mod charters;
pub(crate) mod decisions;
pub(crate) mod delivery_followup;
pub(crate) mod documents;
pub(crate) mod effective_project;
pub(crate) mod genesis;
pub(crate) mod milestones;
pub(crate) mod project_creation;
pub(crate) mod readiness;
pub(crate) mod reconciliation;
pub(crate) mod releases;
pub(crate) mod task_governance;
pub(crate) mod turn_admission;
pub(crate) mod validation;

#[async_trait]
impl ProjectOrchestrationRepo for SqliteDb {
    async fn apply_adaptive_task_command(
        &self,
        input: ApplyAdaptiveTaskCommand,
    ) -> Result<AppliedAdaptiveTaskCommand> {
        task_governance::apply_adaptive_task_command(self, input).await
    }
    async fn insert_project_task_governance_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        governance: CreateProjectTaskGovernance,
    ) -> Result<()> {
        task_governance::insert_project_task_governance_in_tx(self, transaction, governance).await
    }
    async fn create_task_proposal_command(
        &self,
        input: CreateTaskProposalCommand,
    ) -> Result<CreatedTaskProposal> {
        task_governance::create_task_proposal_command(self, input).await
    }
    async fn get_project_charter(&self, id: &str) -> Result<Option<ProjectCharterRecord>> {
        charters::get_project_charter(self, id).await
    }
    async fn get_project_charter_by_project_id(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectCharterRecord>> {
        charters::get_project_charter_by_project_id(self, project_id).await
    }
    async fn get_project_charter_for_account(
        &self,
        id: &str,
        account_id: &str,
    ) -> Result<Option<ProjectCharterRecord>> {
        charters::get_project_charter_for_account(self, id, account_id).await
    }
    async fn create_project_charter(
        &self,
        input: CreateProjectCharter,
    ) -> Result<ProjectCharterRecord> {
        charters::create_project_charter(self, input).await
    }
    async fn get_project_charter_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCharterRevisionRecord>> {
        charters::get_project_charter_revision(self, id).await
    }
    async fn list_project_charter_revisions(
        &self,
        charter_id: &str,
    ) -> Result<Vec<ProjectCharterRevisionRecord>> {
        charters::list_project_charter_revisions(self, charter_id).await
    }
    async fn create_project_charter_revision(
        &self,
        input: CreateProjectCharterRevision,
    ) -> Result<ProjectCharterRevisionRecord> {
        charters::create_project_charter_revision(self, input).await
    }
    async fn create_project_charter_revision_atomically(
        &self,
        input: CreateProjectCharterRevisionAtomically,
    ) -> Result<ProjectCharterRevisionRecord> {
        charters::create_project_charter_revision_atomically(self, input).await
    }
    async fn finalize_project_charter_revision_noop(
        &self,
        input: FinalizeProjectCharterRevisionNoop,
    ) -> Result<ProjectCharterRevisionRecord> {
        charters::finalize_project_charter_revision_noop(self, input).await
    }
    async fn get_project_charter_approval(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCharterApprovalRecord>> {
        charters::get_project_charter_approval(self, id).await
    }
    async fn approve_project_charter(
        &self,
        input: ApproveProjectCharter,
    ) -> Result<ProjectCharterApprovalRecord> {
        charters::approve_project_charter(self, input).await
    }
    async fn apply_project_charter_approval_command(
        &self,
        input: ApplyProjectCharterApprovalCommand,
    ) -> Result<AppliedProjectCharterApprovalRecord> {
        charters::apply_project_charter_approval_command(self, input).await
    }
    async fn create_project_document(
        &self,
        input: CreateProjectDocument,
    ) -> Result<ProjectDocumentRecord> {
        documents::create_project_document(self, input).await
    }
    async fn get_project_document(&self, id: &str) -> Result<Option<ProjectDocumentRecord>> {
        documents::get_project_document(self, id).await
    }
    async fn create_project_document_revision(
        &self,
        input: CreateProjectDocumentRevision,
    ) -> Result<ProjectDocumentRevisionRecord> {
        documents::create_project_document_revision(self, input).await
    }
    async fn create_project_document_atomically(
        &self,
        input: CreateProjectDocumentAtomically,
    ) -> Result<ProjectDocumentRevisionRecord> {
        documents::create_project_document_atomically(self, input).await
    }
    async fn create_project_document_shell_command(
        &self,
        input: CreateProjectDocumentShellCommand,
    ) -> Result<ProjectDocumentRecord> {
        documents::create_project_document_shell_command(self, input).await
    }
    async fn create_project_document_command(
        &self,
        input: CreateProjectDocumentCommand,
    ) -> Result<ProjectDocumentRevisionRecord> {
        documents::create_project_document_command(self, input).await
    }
    async fn append_project_document_revision_command(
        &self,
        input: AppendProjectDocumentRevisionCommand,
    ) -> Result<ProjectDocumentRevisionRecord> {
        documents::append_project_document_revision_command(self, input).await
    }
    async fn get_project_document_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectDocumentRevisionRecord>> {
        documents::get_project_document_revision(self, id).await
    }
    async fn list_project_document_revisions(
        &self,
        document_id: &str,
    ) -> Result<Vec<ProjectDocumentRevisionRecord>> {
        documents::list_project_document_revisions(self, document_id).await
    }
    async fn approve_project_document(
        &self,
        input: ApproveProjectDocument,
    ) -> Result<ProjectDocumentApprovalRecord> {
        documents::approve_project_document(self, input).await
    }
    async fn approve_project_document_command(
        &self,
        input: ApproveProjectDocumentCommand,
    ) -> Result<ProjectDocumentApprovalRecord> {
        documents::approve_project_document_command(self, input).await
    }
    async fn create_project_decision_candidate(
        &self,
        input: CreateProjectDecisionCandidate,
    ) -> Result<ProjectDecisionCandidateRecord> {
        decisions::create_project_decision_candidate(self, input).await
    }
    async fn create_project_decision_candidate_command(
        &self,
        input: CreateProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionCandidateRecord> {
        decisions::create_project_decision_candidate_command(self, input).await
    }
    async fn get_project_decision_candidate(
        &self,
        id: &str,
    ) -> Result<Option<ProjectDecisionCandidateRecord>> {
        decisions::get_project_decision_candidate(self, id).await
    }
    async fn list_project_decision_candidates(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectDecisionCandidateRecord>> {
        decisions::list_project_decision_candidates(self, project_id).await
    }
    async fn append_project_decision(
        &self,
        input: CreateProjectDecision,
    ) -> Result<ProjectDecisionRecord> {
        decisions::append_project_decision(self, input).await
    }
    async fn append_project_decision_command(
        &self,
        input: AppendProjectDecisionCommand,
    ) -> Result<ProjectDecisionRecord> {
        decisions::append_project_decision_command(self, input).await
    }
    async fn approve_project_decision_candidate_command(
        &self,
        input: ApproveProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionRecord> {
        decisions::approve_project_decision_candidate_command(self, input).await
    }
    async fn reject_project_decision_candidate_command(
        &self,
        input: RejectProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionCandidateRecord> {
        decisions::reject_project_decision_candidate_command(self, input).await
    }
    async fn list_effective_project_decisions(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectDecisionRecord>> {
        decisions::list_effective_project_decisions(self, project_id).await
    }
    async fn create_project_milestone(
        &self,
        input: CreateProjectMilestone,
    ) -> Result<ProjectMilestoneRecord> {
        milestones::create_project_milestone(self, input).await
    }
    async fn list_project_milestones(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectMilestoneRecord>> {
        milestones::list_project_milestones(self, project_id).await
    }
    async fn get_project_milestone(&self, id: &str) -> Result<Option<ProjectMilestoneRecord>> {
        milestones::get_project_milestone(self, id).await
    }
    async fn create_project_milestone_revision(
        &self,
        input: CreateProjectMilestoneRevision,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        milestones::create_project_milestone_revision(self, input).await
    }
    async fn create_project_milestone_atomically(
        &self,
        input: CreateProjectMilestoneAtomically,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        milestones::create_project_milestone_atomically(self, input).await
    }
    async fn create_project_milestone_command(
        &self,
        input: CreateProjectMilestoneCommand,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        milestones::create_project_milestone_command(self, input).await
    }
    async fn append_project_milestone_revision_command(
        &self,
        input: AppendProjectMilestoneRevisionCommand,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        milestones::append_project_milestone_revision_command(self, input).await
    }
    async fn set_primary_project_milestone_command(
        &self,
        input: SetPrimaryProjectMilestoneCommand,
    ) -> Result<Project> {
        milestones::set_primary_project_milestone_command(self, input).await
    }
    async fn get_project_milestone_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectMilestoneRevisionRecord>> {
        milestones::get_project_milestone_revision(self, id).await
    }
    async fn list_project_milestone_revisions(
        &self,
        milestone_id: &str,
    ) -> Result<Vec<ProjectMilestoneRevisionRecord>> {
        milestones::list_project_milestone_revisions(self, milestone_id).await
    }
    async fn create_project_milestone_check(
        &self,
        input: CreateProjectMilestoneCheck,
    ) -> Result<ProjectMilestoneCheckRecord> {
        milestones::create_project_milestone_check(self, input).await
    }
    async fn append_project_milestone_check_result(
        &self,
        command: AppendProjectMilestoneCheckResultCommand,
    ) -> Result<ProjectMilestoneCheckResultRecord> {
        milestones::append_project_milestone_check_result(self, command).await
    }
    async fn create_project_readiness_snapshot(
        &self,
        input: CreateProjectReadinessSnapshot,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        readiness::create_project_readiness_snapshot(self, input).await
    }
    async fn create_project_readiness_snapshot_command(
        &self,
        input: CreateProjectReadinessSnapshotCommand,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        readiness::create_project_readiness_snapshot_command(self, input).await
    }
    async fn create_project_release(
        &self,
        input: CreateProjectRelease,
        references: Vec<CreateProjectReleaseReference>,
    ) -> Result<ProjectReleaseRecord> {
        releases::create_project_release(self, input, references).await
    }
    async fn create_project_release_command(
        &self,
        input: CreateProjectReleaseCommand,
    ) -> Result<ProjectReleaseRecord> {
        releases::create_project_release_command(self, input).await
    }
    async fn create_project_release_request_command(
        &self,
        input: CreateProjectReleaseRequestCommand,
    ) -> Result<ProjectReleaseRequestRecord> {
        releases::create_project_release_request_command(self, input).await
    }
    async fn list_project_release_references(
        &self,
        release_id: &str,
    ) -> Result<Vec<ProjectReleaseReferenceRecord>> {
        releases::list_project_release_references(self, release_id).await
    }
    async fn create_project_from_charter_approval(
        &self,
        input: CreateProjectFromCharterApproval,
    ) -> Result<CreatedProjectFromCharterApproval> {
        project_creation::create_project_from_charter_approval(self, input).await
    }
    async fn create_project_canonical_conflict(
        &self,
        input: CreateProjectCanonicalConflict,
    ) -> Result<ProjectCanonicalConflictRecord> {
        reconciliation::create_project_canonical_conflict(self, input).await
    }
    async fn get_project_canonical_conflict(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCanonicalConflictRecord>> {
        reconciliation::get_project_canonical_conflict(self, id).await
    }
    async fn list_project_canonical_conflicts(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectCanonicalConflictRecord>> {
        reconciliation::list_project_canonical_conflicts(self, project_id).await
    }
    async fn create_project_reconciliation(
        &self,
        input: CreateProjectReconciliation,
    ) -> Result<ProjectReconciliationRecord> {
        reconciliation::create_project_reconciliation(self, input).await
    }
    async fn get_project_reconciliation(
        &self,
        id: &str,
    ) -> Result<Option<ProjectReconciliationRecord>> {
        reconciliation::get_project_reconciliation(self, id).await
    }
    async fn list_project_reconciliations(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectReconciliationRecord>> {
        reconciliation::list_project_reconciliations(self, project_id).await
    }
    async fn resolve_project_reconciliation(
        &self,
        input: ResolveProjectReconciliation,
    ) -> Result<ProjectReconciliationRecord> {
        reconciliation::resolve_project_reconciliation(self, input).await
    }
    async fn get_project_reconciliation_resolution(
        &self,
        id: &str,
    ) -> Result<Option<ProjectReconciliationResolutionRecord>> {
        reconciliation::get_project_reconciliation_resolution(self, id).await
    }
}
