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
use sqlx::{Row, Sqlite, Transaction};

const ADAPTIVE_TASK_COMMAND: &str = "task.adaptive";

#[derive(Debug, Clone)]
struct SourceGovernance {
    project_id: String,
    charter_revision_id: Option<String>,
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
         (task_id, project_id, charter_revision_id,
          plan_item_id, milestone_id,
          document_revisions_json, capability_class, risk_class,
          runnable, replacement_of_task_id, provenance_json,
          version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
    )
    .bind(&governance.task_id)
    .bind(&governance.project_id)
    .bind(governance.charter_revision_id.as_deref())
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
        "SELECT g.project_id, g.charter_revision_id,
                g.plan_item_id, g.milestone_id,
                g.document_revisions_json, g.capability_class, g.risk_class,
                g.runnable, g.replacement_of_task_id, g.provenance_json
         FROM project_task_governance g
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
            adaptive_envelope_digest: None,
            fixed_boundary_digest: None,
        });
    };
    let governance = SourceGovernance {
        project_id: row.try_get("project_id")?,
        charter_revision_id: row.try_get("charter_revision_id")?,
        plan_item_id: row.try_get("plan_item_id")?,
        milestone_id: row.try_get("milestone_id")?,
        document_revisions_json: row.try_get("document_revisions_json")?,
        capability_class: row.try_get("capability_class")?,
        risk_class: row.try_get("risk_class")?,
        runnable: row.try_get::<i64, _>("runnable")? == 1,
        provenance_json: row.try_get("provenance_json")?,
    };
    // Readiness authority is the approved Charter plus the milestone's own
    // acceptance matrix. Task-system split/sequence/replace operations are
    // protected by Task/board CAS and receipts/events; they need no separate
    // pinned artifact to authorize them.
    Ok(AdaptiveGate {
        governance: Some(governance),
        envelope: None,
        adaptive_envelope_digest: None,
        fixed_boundary_digest: None,
    })
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
    let (Some(envelope), Some(envelope_digest), Some(boundary_digest)) = (
        gate.envelope.as_ref(),
        gate.adaptive_envelope_digest.as_deref(),
        gate.fixed_boundary_digest.as_deref(),
    ) else {
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
        ("elevated_operations", json!(&envelope.elevated_operations)),
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
                    "reconciliation_required: Task governance fixed boundary '{field}' differs from its adaptive envelope"
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
