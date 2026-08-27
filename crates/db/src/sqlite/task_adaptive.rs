//! Atomic persistence for bounded Task split/sequence/replace commands.
//!
//! This module is deliberately below the TaskService policy preflight.  The
//! preflight is useful for friendly errors, but it is not an authority
//! boundary: the current Charter, source version, and board revision are
//! repeated while a `BEGIN IMMEDIATE` transaction is held.

use super::command_finalization::finalize_command_in_tx;
use super::orchestration::{
    command_event_provenance, resolve_command_replay, validate_command_outcome_identity,
    validate_command_scope, validate_replay_action_bundle,
};
use super::*;
use crate::{
    AdaptiveTaskChild, AdaptiveTaskOperation, AppliedAdaptiveTaskCommand, ApplyAdaptiveTaskCommand,
    CommandReceipt, CommandReceiptRepo, CreateCommandReceipt, CreateDomainEvent,
    CreateProjectTaskGovernance, CreateTask, DbError, DomainEventRepo, Result, Task, TaskMetadata,
    TaskRepo,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};

const ADAPTIVE_TASK_COMMAND: &str = "task.adaptive";
const FIXED_BOUNDARY_DIGEST_SCHEMA: &str = "forge.task-governance/fixed-boundary/v1";
const ADAPTIVE_ENVELOPE_DIGEST_SCHEMA: &str = "forge.task-governance/adaptive-envelope/v1";

#[derive(Debug, Clone)]
struct SourceGovernance {
    project_id: String,
    charter_revision_id: Option<String>,
    baseline_id: Option<String>,
    baseline_revision_id: Option<String>,
    plan_item_id: Option<String>,
    milestone_id: Option<String>,
    document_revisions_json: String,
    capability_class: Option<String>,
    risk_class: Option<String>,
    runnable: bool,
    provenance_json: String,
}

#[derive(Debug, Clone)]
struct AdaptiveGate {
    governance: Option<SourceGovernance>,
    envelope: Option<ParsedAdaptiveEnvelope>,
    baseline_content_digest: Option<String>,
    baseline_rendered_digest: Option<String>,
    release_policy_revision: Option<String>,
    release_policy_digest: Option<String>,
    adaptive_envelope_digest: Option<String>,
    fixed_boundary_digest: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ParsedAdaptiveEnvelope {
    allowed_task_operations: Vec<String>,
    fixed_outcomes: Vec<String>,
    fixed_acceptance: Vec<String>,
    fixed_risk_classes: Vec<String>,
    forbidden_side_effects: Vec<String>,
    elevated_operations: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct FixedBoundary<'a> {
    fixed_outcomes: &'a [String],
    fixed_acceptance: &'a [String],
    fixed_risk_classes: &'a [String],
    forbidden_side_effects: &'a [String],
    release_policy_revision: &'a str,
    release_policy_digest: &'a str,
    elevated_operations: &'a [String],
}

fn canonical_digest_with_schema<T: serde::Serialize>(schema: &str, value: &T) -> Result<String> {
    let value = serde_json::to_value(value).map_err(|error| {
        DbError::Check(format!("canonical adaptive boundary is invalid: {error}"))
    })?;
    let envelope = json!({
        "schema_version": schema,
        "value": canonicalize_json(&value),
    });
    let bytes = serde_json::to_vec(&canonicalize_json(&envelope)).map_err(|error| {
        DbError::Check(format!("canonical adaptive boundary is invalid: {error}"))
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut sorted = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_json(value)))
                .collect::<Vec<_>>();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        scalar => scalar.clone(),
    }
}

fn fixed_boundary_digests(
    envelope: &ParsedAdaptiveEnvelope,
    release_policy_revision: &str,
    release_policy_digest: &str,
) -> Result<(String, String)> {
    let fixed = FixedBoundary {
        fixed_outcomes: &envelope.fixed_outcomes,
        fixed_acceptance: &envelope.fixed_acceptance,
        fixed_risk_classes: &envelope.fixed_risk_classes,
        forbidden_side_effects: &envelope.forbidden_side_effects,
        release_policy_revision,
        release_policy_digest,
        elevated_operations: &envelope.elevated_operations,
    };
    Ok((
        canonical_digest_with_schema(FIXED_BOUNDARY_DIGEST_SCHEMA, &fixed)?,
        canonical_digest_with_schema(ADAPTIVE_ENVELOPE_DIGEST_SCHEMA, envelope)?,
    ))
}

/// Shared governance insertion used by Task proposals and every adaptive
/// child/replacement. Keeping this in the DB boundary makes it impossible for
/// one operation family to silently omit immutable provenance columns.
pub(super) async fn insert_task_governance_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    governance: &CreateProjectTaskGovernance,
) -> Result<()> {
    if governance.task_id.trim().is_empty()
        || governance.project_id.trim().is_empty()
        || governance.document_revisions_json.trim().is_empty()
        || governance.provenance_json.trim().is_empty()
    {
        return Err(DbError::Check(
            "Task governance identity or provenance is incomplete".to_owned(),
        ));
    }
    if serde_json::from_str::<Value>(&governance.document_revisions_json).is_err()
        || serde_json::from_str::<Value>(&governance.provenance_json).is_err()
    {
        return Err(DbError::Check("Task governance JSON is invalid".to_owned()));
    }
    if let Some(replacement_of_task_id) = governance.replacement_of_task_id.as_deref() {
        let owning_project: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM task WHERE id = ? AND deleted_at IS NULL")
                .bind(replacement_of_task_id)
                .fetch_optional(&mut **tx)
                .await?;
        if owning_project.as_deref() != Some(governance.project_id.as_str()) {
            return Err(DbError::Check(
                "Task replacement provenance must reference a Task in the same Project".to_owned(),
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
        .bind(&governance.project_id)
        .bind(charter_revision_id)
        .fetch_one(&mut **tx)
        .await?;
        if admitted != 1 {
            return Err(DbError::Check(
                "runnable Task requires the current approved Project Charter".to_owned(),
            ));
        }
    }
    sqlx::query(
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id, baseline_id,
          baseline_revision_id, plan_item_id, milestone_id,
          document_revisions_json, capability_class, risk_class,
          runnable, replacement_of_task_id, provenance_json,
          version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
    )
    .bind(&governance.task_id)
    .bind(&governance.project_id)
    .bind(governance.charter_revision_id.as_deref())
    .bind(governance.baseline_id.as_deref())
    .bind(governance.baseline_revision_id.as_deref())
    .bind(governance.plan_item_id.as_deref())
    .bind(governance.milestone_id.as_deref())
    .bind(&governance.document_revisions_json)
    .bind(governance.capability_class.as_deref())
    .bind(governance.risk_class.as_deref())
    .bind(if governance.runnable { 1_i64 } else { 0_i64 })
    .bind(governance.replacement_of_task_id.as_deref())
    .bind(&governance.provenance_json)
    .bind(&governance.created_at)
    .bind(&governance.updated_at)
    .execute(&mut **tx)
    .await
    .map_err(super::orchestration::orchestration_write_error)?;
    Ok(())
}

/// Entry point called by the ProjectOrchestrationRepo implementation.
pub(super) async fn apply_adaptive_task_command(
    db: &SqliteDb,
    input: ApplyAdaptiveTaskCommand,
) -> Result<AppliedAdaptiveTaskCommand> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let receipt_input = input
        .command_receipt
        .clone()
        .ok_or_else(|| DbError::Check("adaptive Task command requires a receipt".to_owned()))?;

    // Receipt resolution is deliberately the first operation. Exact retries
    // return the frozen result even if the baseline or source Task changed;
    // changed input/principal/digest returns IdempotencyConflict.
    if let Some(receipt) = resolve_command_replay(db, &mut tx, Some(&receipt_input)).await? {
        validate_command_scope(Some(&receipt_input), "project", &input.project_id)?;
        validate_command_outcome_identity(&receipt, &[("project_id", input.project_id.as_str())])?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let (source_task, tasks, board_revision) = outcome_tasks(&receipt)?;
        if source_task.project_id != input.project_id
            || tasks.iter().any(|task| task.project_id != input.project_id)
        {
            return Err(DbError::IdempotencyConflict);
        }
        tx.commit().await?;
        return Ok(AppliedAdaptiveTaskCommand {
            source_task,
            tasks,
            board_revision,
            receipt,
            replayed: true,
        });
    }

    validate_receipt_input(&receipt_input, &input)?;
    if input.rationale.trim().is_empty() {
        return Err(DbError::Check(
            "adaptive Task rationale is required".to_owned(),
        ));
    }
    let source_row = sqlx::query(&format!(
        "SELECT {TASK_COLUMNS} FROM task
         WHERE id = ? AND project_id = ? AND deleted_at IS NULL"
    ))
    .bind(&input.source_task_id)
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let source = super::map_task(source_row)?;
    if source.archived_at.is_some() {
        return Err(DbError::InvalidTransition);
    }
    if source.version != input.expected_task_version {
        return Err(DbError::TaskVersionConflict {
            expected: input.expected_task_version,
            actual: source.version,
        });
    }
    let actual_board_revision: i64 =
        sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
    if actual_board_revision != input.expected_board_revision {
        return Err(DbError::BoardRevisionConflict {
            expected: input.expected_board_revision,
            actual: actual_board_revision,
        });
    }

    let gate = load_and_validate_adaptive_gate(&mut tx, &source, input.operation.name()).await?;
    let now = receipt_input.committed_at.clone();
    let mut created_tasks = Vec::new();
    match &input.operation {
        AdaptiveTaskOperation::Split { items } => {
            if source.parent_task_id.is_some() {
                return Err(DbError::InvalidTransition);
            }
            if items.is_empty() {
                return Err(DbError::Check(
                    "adaptive Task split requires at least one child".to_owned(),
                ));
            }
            let start_order: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(subtask_order) + 1, 0)
                 FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
            )
            .bind(&source.id)
            .fetch_one(&mut *tx)
            .await?;
            for (offset, item) in items.iter().enumerate() {
                validate_child(item)?;
                let task_id = crate::new_uuid_v4();
                let create = CreateTask {
                    id: task_id,
                    project_id: source.project_id.clone(),
                    repo_id: source.repo_id.clone(),
                    parent_task_id: Some(source.id.clone()),
                    assignee_type: None,
                    assignee_id: None,
                    title: item.title.clone(),
                    description: item.description.clone(),
                    task_type: "sub_task".to_owned(),
                    status: "todo".to_owned(),
                    is_automation: false,
                    priority: 0,
                    subtask_order: Some(start_order + offset as i64),
                    task_state_config: None,
                    merge_config: None,
                    plan: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                };
                let task = TaskRepo::create_in_tx(db, &mut tx, create).await?;
                if let Some(metadata) = TaskMetadata::default().to_json() {
                    sqlx::query(
                        "UPDATE task SET metadata_json = ?, updated_at = ?
                         WHERE id = ? AND project_id = ?",
                    )
                    .bind(&metadata)
                    .bind(&now)
                    .bind(&task.id)
                    .bind(&source.project_id)
                    .execute(&mut *tx)
                    .await?;
                }
                if let Some(governance) = adaptive_child_governance(
                    &gate.governance,
                    &gate,
                    &source,
                    &task.id,
                    "split",
                    &input.rationale,
                    &receipt_input,
                    &now,
                )? {
                    insert_task_governance_in_tx(&mut tx, &governance).await?;
                }
                created_tasks.push(task_snapshot(&mut tx, &task.id).await?);
            }
        }
        AdaptiveTaskOperation::Sequence { ordered_task_ids } => {
            if ordered_task_ids.is_empty() {
                return Err(DbError::Check(
                    "adaptive Task sequence requires at least one subtask".to_owned(),
                ));
            }
            if source.parent_task_id.is_some() {
                return Err(DbError::InvalidTransition);
            }
            validate_sequence(&mut tx, &source, ordered_task_ids).await?;
            for (order, task_id) in ordered_task_ids.iter().enumerate() {
                sqlx::query(
                    "UPDATE task SET subtask_order = ?, updated_at = ?
                     WHERE id = ? AND parent_task_id = ? AND deleted_at IS NULL",
                )
                .bind(order as i64)
                .bind(&now)
                .bind(task_id)
                .bind(&source.id)
                .execute(&mut *tx)
                .await?;
            }
            // `subtask_order` is part of the Task board's sibling ordering but
            // predates the board trigger. Bump the revision explicitly so a
            // stale sequence command cannot overwrite a newer ordering.
            let board_update = sqlx::query(
                "UPDATE project SET board_revision = board_revision + 1
                 WHERE id = ? AND board_revision = ?",
            )
            .bind(&source.project_id)
            .bind(input.expected_board_revision)
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                if error.to_string().contains("constraint") {
                    DbError::VersionConflict
                } else {
                    DbError::from(error)
                }
            })?;
            if board_update.rows_affected() != 1 {
                let actual =
                    sqlx::query_scalar::<_, i64>("SELECT board_revision FROM project WHERE id = ?")
                        .bind(&source.project_id)
                        .fetch_optional(&mut *tx)
                        .await?
                        .ok_or(DbError::NotFound)?;
                return Err(DbError::BoardRevisionConflict {
                    expected: input.expected_board_revision,
                    actual,
                });
            }
            // The sequence result is the source plus its newly ordered
            // children. The source CAS below is still the command's expected
            // Task-version fence.
            created_tasks.clear();
            for task_id in ordered_task_ids {
                created_tasks.push(task_snapshot(&mut tx, task_id).await?);
            }
        }
        AdaptiveTaskOperation::Replace { title, description } => {
            validate_text(title, "replacement title")?;
            let task_id = crate::new_uuid_v4();
            let create = CreateTask {
                id: task_id,
                project_id: source.project_id.clone(),
                repo_id: source.repo_id.clone(),
                parent_task_id: source.parent_task_id.clone(),
                assignee_type: source.assignee_type.clone(),
                assignee_id: source.assignee_id.clone(),
                title: title.clone(),
                description: description.clone(),
                task_type: source.task_type.clone(),
                status: source.status.clone(),
                is_automation: source.is_automation,
                priority: source.priority,
                // A replacement occupies the source's board position. The
                // source remains as the historical record, while the new
                // Task does not silently move to the end of its siblings.
                subtask_order: source.subtask_order,
                task_state_config: source.task_state_config.clone(),
                merge_config: source.merge_config.clone(),
                plan: source.plan.clone(),
                created_at: now.clone(),
                updated_at: now.clone(),
            };
            let task = TaskRepo::create_in_tx(db, &mut tx, create).await?;
            if let Some(governance) = adaptive_child_governance(
                &gate.governance,
                &gate,
                &source,
                &task.id,
                "replace",
                &input.rationale,
                &receipt_input,
                &now,
            )? {
                insert_task_governance_in_tx(&mut tx, &governance).await?;
            }
            created_tasks.push(task_snapshot(&mut tx, &task.id).await?);
        }
    }

    // Source-version CAS is applied after operation validation/materialization;
    // any failure above rolls the complete mutation back. It serializes two
    // adaptive commands that observed the same source Task version.
    let updated = sqlx::query(
        "UPDATE task SET version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND version = ?
           AND deleted_at IS NULL AND archived_at IS NULL",
    )
    .bind(&now)
    .bind(&source.id)
    .bind(&source.project_id)
    .bind(input.expected_task_version)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(DbError::TaskVersionConflict {
            expected: input.expected_task_version,
            actual: sqlx::query_scalar::<_, i64>("SELECT version FROM task WHERE id = ?")
                .bind(&source.id)
                .fetch_optional(&mut *tx)
                .await?
                .unwrap_or(input.expected_task_version),
        });
    }
    let source_after = task_snapshot(&mut tx, &source.id).await?;
    let board_revision: i64 = sqlx::query_scalar("SELECT board_revision FROM project WHERE id = ?")
        .bind(&source.project_id)
        .fetch_one(&mut *tx)
        .await?;

    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            Some(&receipt_input),
            receipt_input.principal_type.clone(),
            Some(receipt_input.principal_id.clone()),
            receipt_input.correlation_id.clone(),
            receipt_input.causation_id.clone(),
            receipt_input.causation_depth,
        );
    let task_ids = created_tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<Vec<_>>();
    let event = CreateDomainEvent {
        id: crate::new_uuid_v4(),
        event_type: ADAPTIVE_TASK_COMMAND.to_owned(),
        entity_type: "task".to_owned(),
        entity_id: source.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: source.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("task-adaptive:{}", receipt_input.id)),
        payload_json: json!({
            "operation": input.operation.name(),
            "project_id": source.project_id,
            "source_task_id": source.id,
            "task_ids": task_ids,
            "rationale": input.rationale,
            "actor": {
                "type": receipt_input.principal_type,
                "id": receipt_input.principal_id,
            },
            "governance": {
                "baseline_content_digest": gate.baseline_content_digest,
                "baseline_rendered_digest": gate.baseline_rendered_digest,
                "release_policy_revision": gate.release_policy_revision,
                "release_policy_digest": gate.release_policy_digest,
                "adaptive_envelope_digest": gate.adaptive_envelope_digest,
                "fixed_boundary_digest": gate.fixed_boundary_digest,
                "adaptive_envelope": gate.envelope,
            },
            "expected_task_version": input.expected_task_version,
            "expected_board_revision": input.expected_board_revision,
            "board_revision": board_revision,
        })
        .to_string(),
        created_at: now.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;

    let mut receipt = input.command_receipt;
    let mut action_execution = input.action_execution;
    if let Some(receipt) = receipt.as_mut() {
        receipt.outcome_json = json!({
            "operation": input.operation.name(),
            "project_id": source_after.project_id,
            "source_task_id": source_after.id,
            "source_task": source_after,
            "tasks": created_tasks,
            "board_revision": board_revision,
            "rationale": input.rationale,
        })
        .to_string();
        if let Some(execution) = action_execution.as_mut() {
            execution.result_json = Some(receipt.outcome_json.clone());
            execution.action_outcome_json = Some(receipt.outcome_json.clone());
        }
    }
    finalize_command_in_tx(db, &mut tx, &event.id, receipt, action_execution).await?;
    let persisted = CommandReceiptRepo::get_command_receipt_in_tx(
        db,
        &mut tx,
        &receipt_input.principal_type,
        &receipt_input.principal_id,
        &receipt_input.scope_type,
        &receipt_input.scope_id,
        &receipt_input.operation,
        &receipt_input.idempotency_key,
        &receipt_input.input_digest,
    )
    .await?
    .ok_or(DbError::IdempotencyConflict)?;
    tx.commit().await?;
    Ok(AppliedAdaptiveTaskCommand {
        source_task: source_after,
        tasks: created_tasks,
        board_revision,
        receipt: persisted,
        replayed: false,
    })
}

fn validate_receipt_input(
    receipt: &CreateCommandReceipt,
    input: &ApplyAdaptiveTaskCommand,
) -> Result<()> {
    if receipt.operation != ADAPTIVE_TASK_COMMAND
        || receipt.scope_type != "project"
        || receipt.scope_id != input.project_id
        || receipt.principal_type.trim().is_empty()
        || receipt.principal_id.trim().is_empty()
        || receipt.correlation_id.trim().is_empty()
        || receipt.idempotency_key.trim().is_empty()
        || receipt.input_digest.trim().is_empty()
        || receipt.policy_result != "allowed"
    {
        return Err(DbError::IdempotencyConflict);
    }
    if receipt.causation_depth < 0 || receipt.causation_depth > 16 {
        return Err(DbError::Check(
            "adaptive Task causation depth is outside the allowed range".to_owned(),
        ));
    }
    Ok(())
}

async fn load_and_validate_adaptive_gate(
    tx: &mut Transaction<'_, Sqlite>,
    source: &Task,
    _operation: &str,
) -> Result<AdaptiveGate> {
    let governance_row = sqlx::query(
        "SELECT g.project_id, g.charter_revision_id, g.baseline_id,
                g.baseline_revision_id, g.plan_item_id, g.milestone_id,
                g.document_revisions_json, g.capability_class, g.risk_class,
                g.runnable, g.replacement_of_task_id, g.provenance_json,
                r.adaptive_envelope_json, r.content_digest,
                r.rendered_digest, r.release_policy_revision,
                r.release_policy_digest
         FROM project_task_governance g
         LEFT JOIN project_execution_baseline_revision r
           ON r.id = g.baseline_revision_id AND r.baseline_id = g.baseline_id
         WHERE g.task_id = ? AND g.project_id = ?",
    )
    .bind(&source.id)
    .bind(&source.project_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = governance_row else {
        // Legacy Tasks retain their historical adaptive behavior; their
        // command is still protected by Task/board CAS and receipts/events.
        return Ok(AdaptiveGate {
            governance: None,
            envelope: None,
            baseline_content_digest: None,
            baseline_rendered_digest: None,
            release_policy_revision: None,
            release_policy_digest: None,
            adaptive_envelope_digest: None,
            fixed_boundary_digest: None,
        });
    };
    let governance = SourceGovernance {
        project_id: row.try_get("project_id")?,
        charter_revision_id: row.try_get("charter_revision_id")?,
        baseline_id: row.try_get("baseline_id")?,
        baseline_revision_id: row.try_get("baseline_revision_id")?,
        plan_item_id: row.try_get("plan_item_id")?,
        milestone_id: row.try_get("milestone_id")?,
        document_revisions_json: row.try_get("document_revisions_json")?,
        capability_class: row.try_get("capability_class")?,
        risk_class: row.try_get("risk_class")?,
        runnable: row.try_get::<i64, _>("runnable")? == 1,
        provenance_json: row.try_get("provenance_json")?,
    };
    let Some(baseline_id) = governance.baseline_id.as_deref() else {
        // Charter-backed Tasks do not need a baseline for normal Task-system
        // split/sequence/replace operations.
        return Ok(AdaptiveGate {
            governance: Some(governance),
            envelope: None,
            baseline_content_digest: None,
            baseline_rendered_digest: None,
            release_policy_revision: None,
            release_policy_digest: None,
            adaptive_envelope_digest: None,
            fixed_boundary_digest: None,
        });
    };
    let Some(baseline_revision_id) = governance.baseline_revision_id.as_deref() else {
        return Err(DbError::Check(
            "reconciliation_required: adaptive Task governance has no baseline revision".to_owned(),
        ));
    };
    let exact_baseline: Option<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT p.current_charter_revision_id, r.id,
                r.charter_revision_id, r.adaptive_envelope_json,
                r.content_digest
         FROM project p
         JOIN project_execution_baseline b ON b.project_id = p.id
         JOIN project_execution_baseline_revision r
           ON r.baseline_id = b.id AND r.id = ?
         WHERE p.id = ? AND b.id = ?
           AND p.charter_status = 'charter_backed'
           AND p.charter_setup_required = 0",
    )
    .bind(baseline_revision_id)
    .bind(&source.project_id)
    .bind(baseline_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((
        charter_revision_id,
        current_revision_id,
        baseline_charter_revision_id,
        envelope_json,
        content_digest,
    )) = exact_baseline
    else {
        return Err(DbError::Check(
            "reconciliation_required: adaptive Task references unavailable baseline traceability"
                .to_owned(),
        ));
    };
    if governance.charter_revision_id.as_deref() != Some(charter_revision_id.as_str())
        || baseline_charter_revision_id != charter_revision_id
        || current_revision_id != baseline_revision_id
    {
        return Err(DbError::Check(
            "reconciliation_required: adaptive Task Charter/baseline binding is stale".to_owned(),
        ));
    }
    let revision_digests: (String, String, String, String) = sqlx::query_as(
        "SELECT content_digest, rendered_digest, release_policy_revision,
                release_policy_digest
         FROM project_execution_baseline_revision
         WHERE id = ? AND baseline_id = ?",
    )
    .bind(baseline_revision_id)
    .bind(baseline_id)
    .fetch_one(&mut **tx)
    .await?;
    if revision_digests.0 != content_digest {
        return Err(DbError::Check(
            "reconciliation_required: baseline digest changed while admitting adaptive Task"
                .to_owned(),
        ));
    }
    let baseline_projection: (
        String,
        String,
        String,
        Option<String>,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT document_revisions_json, plan_items_json, milestone_ids_json,
                    primary_milestone_id, capability_classes_json, risk_classes_json,
                    elevated_operations_json
             FROM project_execution_baseline_revision
             WHERE id = ? AND baseline_id = ?",
    )
    .bind(baseline_revision_id)
    .bind(baseline_id)
    .fetch_one(&mut **tx)
    .await?;
    let envelope = parse_adaptive_envelope(&envelope_json)?;
    let baseline_elevated: Value =
        serde_json::from_str(&baseline_projection.6).map_err(|error| {
            DbError::Check(format!(
                "reconciliation_required: baseline elevated operations are invalid: {error}"
            ))
        })?;
    if baseline_elevated != json!(envelope.elevated_operations) {
        return Err(DbError::Check(
            "reconciliation_required: baseline elevated operations differ from its adaptive envelope"
                .to_owned(),
        ));
    }
    validate_source_against_baseline(
        &governance,
        &envelope,
        &baseline_projection,
        &revision_digests.0,
        &revision_digests.1,
        &revision_digests.2,
        &revision_digests.3,
    )?;
    let (fixed_boundary_digest, adaptive_envelope_digest) =
        fixed_boundary_digests(&envelope, &revision_digests.2, &revision_digests.3)?;
    Ok(AdaptiveGate {
        governance: Some(governance),
        envelope: Some(envelope),
        baseline_content_digest: Some(revision_digests.0),
        baseline_rendered_digest: Some(revision_digests.1),
        release_policy_revision: Some(revision_digests.2),
        release_policy_digest: Some(revision_digests.3),
        adaptive_envelope_digest: Some(adaptive_envelope_digest),
        fixed_boundary_digest: Some(fixed_boundary_digest),
    })
}

fn parse_adaptive_envelope(value: &str) -> Result<ParsedAdaptiveEnvelope> {
    let parsed: Value = serde_json::from_str(value).map_err(|error| {
        DbError::Check(format!(
            "reconciliation_required: governing adaptive envelope is invalid: {error}"
        ))
    })?;
    let object = parsed.as_object().ok_or_else(|| {
        DbError::Check(
            "reconciliation_required: governing adaptive envelope must be an object".to_owned(),
        )
    })?;
    const FIELDS: [&str; 6] = [
        "allowed_task_operations",
        "fixed_outcomes",
        "fixed_acceptance",
        "fixed_risk_classes",
        "forbidden_side_effects",
        "elevated_operations",
    ];
    if object.len() != FIELDS.len() || FIELDS.iter().any(|field| !object.contains_key(*field)) {
        return Err(DbError::Check(
            "reconciliation_required: governing adaptive envelope is incomplete".to_owned(),
        ));
    }
    if FIELDS.iter().any(|field| {
        !object.get(*field).is_some_and(Value::is_array)
            || object
                .get(*field)
                .and_then(Value::as_array)
                .is_some_and(|values| values.iter().any(|value| !value.is_string()))
    }) {
        return Err(DbError::Check(
            "reconciliation_required: governing adaptive envelope contains a non-string boundary"
                .to_owned(),
        ));
    }
    let strings = |field: &str| -> Result<Vec<String>> {
        object
            .get(field)
            .and_then(Value::as_array)
            .ok_or_else(|| DbError::Check("adaptive envelope field is not an array".to_owned()))
            .and_then(|values| {
                values
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            DbError::Check("adaptive envelope boundary is not a string".to_owned())
                        })
                    })
                    .collect()
            })
    };
    Ok(ParsedAdaptiveEnvelope {
        allowed_task_operations: strings("allowed_task_operations")?,
        fixed_outcomes: strings("fixed_outcomes")?,
        fixed_acceptance: strings("fixed_acceptance")?,
        fixed_risk_classes: strings("fixed_risk_classes")?,
        forbidden_side_effects: strings("forbidden_side_effects")?,
        elevated_operations: strings("elevated_operations")?,
    })
}

fn validate_source_against_baseline(
    governance: &SourceGovernance,
    envelope: &ParsedAdaptiveEnvelope,
    baseline_projection: &(
        String,
        String,
        String,
        Option<String>,
        String,
        String,
        String,
    ),
    baseline_content_digest: &str,
    baseline_rendered_digest: &str,
    release_policy_revision: &str,
    release_policy_digest: &str,
) -> Result<()> {
    let provenance: Value = serde_json::from_str(&governance.provenance_json).map_err(|error| {
        DbError::Check(format!(
            "reconciliation_required: Task governance provenance is invalid: {error}"
        ))
    })?;
    let provenance = provenance.as_object().ok_or_else(|| {
        DbError::Check(
            "reconciliation_required: Task governance provenance must be an object".to_owned(),
        )
    })?;
    if provenance.contains_key("fixed_risk_class") {
        return Err(DbError::Check(
            "reconciliation_required: singular fixed_risk_class provenance is unsupported"
                .to_owned(),
        ));
    }
    let (fixed_boundary_digest, adaptive_envelope_digest) =
        fixed_boundary_digests(envelope, release_policy_revision, release_policy_digest)?;
    let expected = [
        ("fixed_outcomes", json!(envelope.fixed_outcomes)),
        ("fixed_acceptance", json!(envelope.fixed_acceptance)),
        ("fixed_risk_classes", json!(envelope.fixed_risk_classes)),
        (
            "forbidden_side_effects",
            json!(envelope.forbidden_side_effects),
        ),
        (
            "release_policy_revision",
            Value::String(release_policy_revision.to_owned()),
        ),
        (
            "release_policy_digest",
            Value::String(release_policy_digest.to_owned()),
        ),
        ("elevated_operations", json!(envelope.elevated_operations)),
        (
            "governing_baseline_content_digest",
            Value::String(baseline_content_digest.to_owned()),
        ),
        (
            "governing_baseline_rendered_digest",
            Value::String(baseline_rendered_digest.to_owned()),
        ),
    ];
    for (field, value) in expected {
        if let Some(existing) = provenance.get(field) {
            if existing != &value {
                return Err(DbError::Check(format!(
                    "reconciliation_required: Task governance fixed boundary '{field}' differs from the active baseline"
                )));
            }
        }
    }
    for (field, expected) in [
        ("fixed_boundary_digest", fixed_boundary_digest),
        ("adaptive_envelope_digest", adaptive_envelope_digest),
    ] {
        if let Some(existing) = provenance.get(field) {
            if existing.as_str() != Some(expected.as_str()) {
                return Err(DbError::Check(format!(
                    "reconciliation_required: Task governance digest '{field}' differs from the active baseline"
                )));
            }
        }
    }
    if let Some(risk) = governance
        .risk_class
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        if !envelope
            .fixed_risk_classes
            .iter()
            .any(|value| value == risk)
        {
            return Err(DbError::Check(
                "reconciliation_required: Task risk class differs from the active fixed boundary"
                    .to_owned(),
            ));
        }
    }
    if let Some(capability) = governance.capability_class.as_deref() {
        if json_array_declares_values(&baseline_projection.4)
            && !json_contains_identifier(&baseline_projection.4, capability)
        {
            return Err(DbError::Check(
                "reconciliation_required: Task capability class differs from the active baseline"
                    .to_owned(),
            ));
        }
    }
    if let Some(risk) = governance.risk_class.as_deref() {
        if json_array_declares_values(&baseline_projection.5)
            && !json_contains_identifier(&baseline_projection.5, risk)
        {
            return Err(DbError::Check(
                "reconciliation_required: Task risk class differs from the active baseline"
                    .to_owned(),
            ));
        }
    }
    if let Some(plan_item_id) = governance.plan_item_id.as_deref() {
        if !json_contains_identifier(&baseline_projection.1, plan_item_id) {
            return Err(DbError::Check(
                "reconciliation_required: Task plan item differs from the active baseline"
                    .to_owned(),
            ));
        }
    }
    if let Some(milestone_id) = governance.milestone_id.as_deref() {
        let in_milestones = json_contains_identifier(&baseline_projection.2, milestone_id)
            || baseline_projection.3.as_deref() == Some(milestone_id);
        if !in_milestones {
            return Err(DbError::Check(
                "reconciliation_required: Task milestone differs from the active baseline"
                    .to_owned(),
            ));
        }
    }
    let document_revisions: Value = serde_json::from_str(&governance.document_revisions_json)
        .map_err(|error| {
            DbError::Check(format!(
                "reconciliation_required: Task document references are invalid: {error}"
            ))
        })?;
    if let Value::Array(revisions) = document_revisions {
        for revision in revisions {
            let identifier = revision
                .as_str()
                .or_else(|| revision.get("id").and_then(Value::as_str))
                .or_else(|| revision.get("document_revision_id").and_then(Value::as_str));
            if let Some(identifier) = identifier {
                if !json_contains_identifier(&baseline_projection.0, identifier) {
                    return Err(DbError::Check(
                        "reconciliation_required: Task document reference differs from the active baseline"
                            .to_owned(),
                    ));
                }
            }
        }
    } else {
        return Err(DbError::Check(
            "reconciliation_required: Task document references must be an array".to_owned(),
        ));
    }
    Ok(())
}

fn json_contains_identifier(raw: &str, identifier: &str) -> bool {
    fn contains(value: &Value, identifier: &str) -> bool {
        match value {
            Value::String(value) => value == identifier,
            Value::Array(values) => values.iter().any(|value| contains(value, identifier)),
            Value::Object(values) => values.values().any(|value| contains(value, identifier)),
            Value::Null | Value::Bool(_) | Value::Number(_) => false,
        }
    }
    serde_json::from_str::<Value>(raw)
        .ok()
        .is_some_and(|value| contains(&value, identifier))
}

fn json_array_declares_values(raw: &str) -> bool {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| value.as_array().map(|values| !values.is_empty()))
        .unwrap_or(true)
}

fn apply_authoritative_provenance_fields(
    provenance: &mut Map<String, Value>,
    gate: &AdaptiveGate,
) -> Result<()> {
    if provenance.contains_key("fixed_risk_class") {
        return Err(DbError::Check(
            "reconciliation_required: singular fixed_risk_class provenance is unsupported"
                .to_owned(),
        ));
    }
    let (
        Some(envelope),
        Some(content_digest),
        Some(rendered_digest),
        Some(policy_revision),
        Some(policy_digest),
        Some(envelope_digest),
        Some(boundary_digest),
    ) = (
        gate.envelope.as_ref(),
        gate.baseline_content_digest.as_deref(),
        gate.baseline_rendered_digest.as_deref(),
        gate.release_policy_revision.as_deref(),
        gate.release_policy_digest.as_deref(),
        gate.adaptive_envelope_digest.as_deref(),
        gate.fixed_boundary_digest.as_deref(),
    )
    else {
        return Ok(());
    };
    let authoritative = [
        ("fixed_outcomes", json!(&envelope.fixed_outcomes)),
        ("fixed_acceptance", json!(&envelope.fixed_acceptance)),
        ("fixed_risk_classes", json!(&envelope.fixed_risk_classes)),
        (
            "forbidden_side_effects",
            json!(&envelope.forbidden_side_effects),
        ),
        (
            "release_policy_revision",
            Value::String(policy_revision.to_owned()),
        ),
        (
            "release_policy_digest",
            Value::String(policy_digest.to_owned()),
        ),
        ("elevated_operations", json!(&envelope.elevated_operations)),
        (
            "governing_baseline_content_digest",
            Value::String(content_digest.to_owned()),
        ),
        (
            "governing_baseline_rendered_digest",
            Value::String(rendered_digest.to_owned()),
        ),
        (
            "adaptive_envelope_digest",
            Value::String(envelope_digest.to_owned()),
        ),
        (
            "fixed_boundary_digest",
            Value::String(boundary_digest.to_owned()),
        ),
    ];
    for (field, value) in authoritative {
        if let Some(existing) = provenance.get(field) {
            if existing != &value {
                return Err(DbError::Check(format!(
                    "reconciliation_required: Task governance fixed boundary '{field}' differs from the active baseline"
                )));
            }
        }
        provenance.insert(field.to_owned(), value);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn adaptive_child_governance(
    source_governance: &Option<SourceGovernance>,
    gate: &AdaptiveGate,
    source: &Task,
    child_task_id: &str,
    operation: &str,
    rationale: &str,
    receipt: &CreateCommandReceipt,
    now: &str,
) -> Result<Option<CreateProjectTaskGovernance>> {
    let Some(source_governance) = source_governance.as_ref() else {
        return Ok(None);
    };
    let mut provenance: Map<String, Value> =
        serde_json::from_str(&source_governance.provenance_json).map_err(|error| {
            DbError::Check(format!("Task governance provenance is invalid: {error}"))
        })?;
    apply_authoritative_provenance_fields(&mut provenance, gate)?;
    provenance.insert(
        "origin_task_id".to_owned(),
        Value::String(source.id.clone()),
    );
    provenance.insert(
        "replacement_of_task_id".to_owned(),
        Value::String(source.id.clone()),
    );
    provenance.insert(
        "adaptive_operation".to_owned(),
        Value::String(operation.to_owned()),
    );
    provenance.insert(
        "adaptive_rationale".to_owned(),
        Value::String(rationale.to_owned()),
    );
    provenance.insert(
        "adaptive_actor_type".to_owned(),
        Value::String(receipt.principal_type.clone()),
    );
    provenance.insert(
        "adaptive_actor_id".to_owned(),
        Value::String(receipt.principal_id.clone()),
    );
    Ok(Some(CreateProjectTaskGovernance {
        task_id: child_task_id.to_owned(),
        project_id: source_governance.project_id.clone(),
        charter_revision_id: source_governance.charter_revision_id.clone(),
        baseline_id: source_governance.baseline_id.clone(),
        baseline_revision_id: source_governance.baseline_revision_id.clone(),
        plan_item_id: source_governance.plan_item_id.clone(),
        milestone_id: source_governance.milestone_id.clone(),
        document_revisions_json: source_governance.document_revisions_json.clone(),
        capability_class: source_governance.capability_class.clone(),
        risk_class: source_governance.risk_class.clone(),
        runnable: source_governance.runnable,
        replacement_of_task_id: Some(source.id.clone()),
        provenance_json: serde_json::to_string(&Value::Object(provenance))
            .map_err(|error| DbError::Check(error.to_string()))?,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    }))
}

/// Keep the historical `task.propose` surface available for ordinary Tasks,
/// but make a governed root Task use the same adaptive gate as split.  The
/// service preflight performs the same check for friendly errors; this DB
/// helper is the authority that closes the race and prevents a direct adapter
/// from reshaping a governed parent through the generic proposal command.
pub(super) async fn validate_parent_proposal_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task: &CreateTask,
    governance: Option<CreateProjectTaskGovernance>,
) -> Result<Option<CreateProjectTaskGovernance>> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(governance);
    };
    let parent_row = sqlx::query(&format!(
        "SELECT {TASK_COLUMNS} FROM task
         WHERE id = ? AND project_id = ? AND deleted_at IS NULL"
    ))
    .bind(parent_task_id)
    .bind(&task.project_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let parent = super::map_task(parent_row)?;
    if parent.parent_task_id.is_some() || parent.archived_at.is_some() {
        return Err(DbError::InvalidTransition);
    }
    let parent_governance_exists: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM project_task_governance
         WHERE task_id = ? AND project_id = ? LIMIT 1",
    )
    .bind(&parent.id)
    .bind(&task.project_id)
    .fetch_optional(&mut **tx)
    .await?;
    if parent_governance_exists.is_none() {
        return Ok(governance);
    }
    let baseline_id: Option<String> = sqlx::query_scalar(
        "SELECT baseline_id FROM project_task_governance
         WHERE task_id = ? AND project_id = ?",
    )
    .bind(&parent.id)
    .bind(&task.project_id)
    .fetch_one(&mut **tx)
    .await?;
    if baseline_id.is_none() {
        return Ok(governance);
    }
    // A generic parent proposal is admitted only when it carries the exact
    // source governance projection and passes the same active adaptive gate;
    // missing or changed governance cannot use `task.propose` as a bypass.
    let gate = load_and_validate_adaptive_gate(tx, &parent, "split").await?;
    let source = gate.governance.as_ref().ok_or_else(|| {
        DbError::Check(
            "reconciliation_required: governed adaptive parent provenance is unavailable"
                .to_owned(),
        )
    })?;
    let mut requested = governance.ok_or_else(|| {
        DbError::Check(
            "reconciliation_required: governed parent reshaping requires the adaptive Task command"
                .to_owned(),
        )
    })?;
    if requested.task_id != task.id || requested.project_id != task.project_id {
        return Err(DbError::IdempotencyConflict);
    }
    if requested.charter_revision_id != source.charter_revision_id
        || requested.baseline_id != source.baseline_id
        || requested.baseline_revision_id != source.baseline_revision_id
        || requested.plan_item_id != source.plan_item_id
        || requested.milestone_id != source.milestone_id
        || requested.document_revisions_json != source.document_revisions_json
        || requested.capability_class != source.capability_class
        || requested.risk_class != source.risk_class
        || requested.runnable != source.runnable
    {
        return Err(DbError::Check(
            "reconciliation_required: generic Task proposal changes governed parent provenance"
                .to_owned(),
        ));
    }
    let mut provenance: Map<String, Value> = serde_json::from_str(&requested.provenance_json)
        .map_err(|error| {
            DbError::Check(format!("Task governance provenance is invalid: {error}"))
        })?;
    apply_authoritative_provenance_fields(&mut provenance, &gate)?;
    requested.provenance_json = serde_json::to_string(&Value::Object(provenance))
        .map_err(|error| DbError::Check(error.to_string()))?;
    Ok(Some(requested))
}

fn validate_child(item: &AdaptiveTaskChild) -> Result<()> {
    validate_text(&item.title, "child title")?;
    if item
        .description
        .as_deref()
        .is_some_and(|description| description.len() > 64 * 1024)
    {
        return Err(DbError::Check(
            "adaptive Task child description is too large".to_owned(),
        ));
    }
    Ok(())
}

fn validate_text(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(DbError::Check(format!("adaptive Task {field} is required")));
    }
    Ok(())
}

async fn validate_sequence(
    tx: &mut Transaction<'_, Sqlite>,
    source: &Task,
    ordered_task_ids: &[String],
) -> Result<()> {
    let sibling_ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM task WHERE parent_task_id = ? AND deleted_at IS NULL
         ORDER BY subtask_order ASC, created_at ASC, id ASC",
    )
    .bind(&source.id)
    .fetch_all(&mut **tx)
    .await?;
    let submitted = ordered_task_ids
        .iter()
        .collect::<std::collections::HashSet<_>>();
    if submitted.len() != ordered_task_ids.len()
        || submitted.len() != sibling_ids.len()
        || sibling_ids.iter().any(|id| !submitted.contains(id))
    {
        return Err(DbError::Check(
            "adaptive Task sequence must contain exactly the source's active subtasks".to_owned(),
        ));
    }
    Ok(())
}

async fn task_snapshot(tx: &mut Transaction<'_, Sqlite>, task_id: &str) -> Result<Task> {
    let row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(task_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(DbError::from)?;
    super::map_task(row)
}

fn outcome_tasks(receipt: &CommandReceipt) -> Result<(Task, Vec<Task>, i64)> {
    let outcome: Value =
        serde_json::from_str(&receipt.outcome_json).map_err(|_| DbError::IdempotencyConflict)?;
    let source_task: Task = serde_json::from_value(
        outcome
            .get("source_task")
            .cloned()
            .ok_or(DbError::IdempotencyConflict)?,
    )
    .map_err(|_| DbError::IdempotencyConflict)?;
    let tasks: Vec<Task> = serde_json::from_value(
        outcome
            .get("tasks")
            .cloned()
            .ok_or(DbError::IdempotencyConflict)?,
    )
    .map_err(|_| DbError::IdempotencyConflict)?;
    let board_revision = outcome
        .get("board_revision")
        .and_then(Value::as_i64)
        .ok_or(DbError::IdempotencyConflict)?;
    Ok((source_task, tasks, board_revision))
}
