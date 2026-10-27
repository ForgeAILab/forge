use super::*;
/// Recheck mutable source-actor and executor authorization while the proposal
/// transaction owns SQLite's writer lock, closing races with binding or
/// Project membership changes after service preflight.
async fn recheck_task_proposal_authorization_in_tx(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    input: &CreateTaskProposalCommand,
    receipt: &CreateCommandReceipt,
) -> Result<()> {
    if input.source_operation != "task.propose"
        || input.source_requested_permission != "propose_task"
        || input.source_policy_result != "allowed"
        || input.source_target_type.as_deref() != Some("project")
        || input.source_target_id.as_deref() != Some(input.task.project_id.as_str())
        || receipt.policy_result != input.source_policy_result
    {
        return Err(DbError::IdempotencyConflict);
    }
    let (actor_identity_id, scope_type, scope_id, operation, requested_permission, policy_result) = (
        input.source_actor_identity_id.clone(),
        input.source_scope_type.clone(),
        input.source_scope_id.clone(),
        input.source_operation.clone(),
        input.source_requested_permission.clone(),
        input.source_policy_result.clone(),
    );

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
    let permits_task_proposal = permission_ceiling_contains(&permission_ceiling, "propose_task");
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

    Ok(())
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct AdaptiveTaskGovernanceSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl AdaptiveTaskGovernanceSourceRow {
    pub fn charter_revision_id_unchecked(&self) -> Option<String> {
        self.row.get("charter_revision_id")
    }
    pub fn plan_item_id_unchecked(&self) -> Option<String> {
        self.row.get("plan_item_id")
    }
    pub fn milestone_id_unchecked(&self) -> Option<String> {
        self.row.get("milestone_id")
    }
    pub fn document_revisions_json_unchecked(&self) -> String {
        self.row.get("document_revisions_json")
    }
    pub fn capability_class_unchecked(&self) -> Option<String> {
        self.row.get("capability_class")
    }
    pub fn risk_class_unchecked(&self) -> Option<String> {
        self.row.get("risk_class")
    }
    pub fn provenance_json_unchecked(&self) -> String {
        self.row.get("provenance_json")
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct TaskExecutionCharterGovernanceSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl TaskExecutionCharterGovernanceSourceRow {
    pub fn charter_status_unchecked(&self) -> String {
        self.row.get("charter_status")
    }
    pub fn charter_setup_required_unchecked(&self) -> i64 {
        self.row.get("charter_setup_required")
    }
    pub fn current_charter_revision_id_unchecked(&self) -> Option<String> {
        self.row.get("current_charter_revision_id")
    }
    pub fn task_type_unchecked(&self) -> String {
        self.row.get("task_type")
    }
    pub fn charter_revision_id_unchecked(&self) -> Option<String> {
        self.row.get("charter_revision_id")
    }
    pub fn capability_class_unchecked(&self) -> Option<String> {
        self.row.get("capability_class")
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct WorkspaceLeaseCharterGateRow {
    row: sqlx::sqlite::SqliteRow,
}

impl WorkspaceLeaseCharterGateRow {
    pub fn charter_status_unchecked(&self) -> String {
        self.row.get("charter_status")
    }
    pub fn charter_setup_required_unchecked(&self) -> i64 {
        self.row.get("charter_setup_required")
    }
    pub fn current_charter_revision_id_unchecked(&self) -> Option<String> {
        self.row.get("current_charter_revision_id")
    }
    pub fn charter_revision_id_unchecked(&self) -> Option<String> {
        self.row.get("charter_revision_id")
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct TaskGovernanceDocumentRevisionSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl TaskGovernanceDocumentRevisionSourceRow {
    pub fn project_id_unchecked(&self) -> String {
        self.row.get("project_id")
    }
    pub fn lifecycle_unchecked(&self) -> String {
        self.row.get("lifecycle")
    }
}

impl SqliteDb {
    pub async fn adaptive_task_governance_source(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<AdaptiveTaskGovernanceSourceRow>> {
        let rows = sqlx::query(
            "SELECT g.project_id, g.charter_revision_id, g.plan_item_id,
                    g.milestone_id, g.document_revisions_json,
                    g.capability_class, g.risk_class, g.runnable,
                    g.provenance_json
             FROM project_task_governance g
             WHERE g.task_id = ? AND g.project_id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| AdaptiveTaskGovernanceSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn task_governance_milestone_project(
        &self,
        milestone_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>(
                "SELECT project_id FROM project_milestone WHERE id = ?",
            )
            .bind(milestone_id)
            .fetch_optional(self.pool())
            .await?,
        )
    }
}

impl SqliteDb {
    pub async fn task_execution_charter_governance_source(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<TaskExecutionCharterGovernanceSourceRow>> {
        let rows = sqlx::query(
            "SELECT p.charter_status, p.charter_setup_required,
                    p.current_charter_revision_id,
                    t.task_type,
                    g.charter_revision_id,
                    g.capability_class
             FROM project p
             JOIN task t ON t.id = ? AND t.project_id = p.id
                 AND t.deleted_at IS NULL
             LEFT JOIN project_task_governance g ON g.project_id = p.id
                 AND g.task_id = t.id
             WHERE p.id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| TaskExecutionCharterGovernanceSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn workspace_lease_task_capability_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT capability_class FROM project_task_governance
             WHERE task_id = ? AND project_id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_lease_charter_gate_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<WorkspaceLeaseCharterGateRow>> {
        let rows = sqlx::query(
            "SELECT p.charter_status, p.charter_setup_required,
                    p.current_charter_revision_id, g.charter_revision_id
             FROM project p
             LEFT JOIN project_task_governance g
               ON g.project_id = p.id AND g.task_id = ?
             WHERE p.id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| WorkspaceLeaseCharterGateRow { row }))
    }
}

impl SqliteDb {
    pub async fn workspace_lease_task_capability(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT capability_class FROM project_task_governance
             WHERE task_id = ? AND project_id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub async fn task_governance_document_revision_source_on_pool(
    pool: &SqlitePool,
    revision_id: &str,
) -> crate::Result<Option<TaskGovernanceDocumentRevisionSourceRow>> {
    let rows = sqlx::query(
        "SELECT d.project_id, r.lifecycle
             FROM project_document_revision r
             JOIN project_document d ON d.id = r.document_id
             WHERE r.id = ?",
    )
    .bind(revision_id)
    .fetch_optional(pool)
    .await?;
    Ok(rows.map(|row| TaskGovernanceDocumentRevisionSourceRow { row }))
}
pub(super) async fn apply_adaptive_task_command(
    db: &SqliteDb,
    input: ApplyAdaptiveTaskCommand,
) -> Result<AppliedAdaptiveTaskCommand> {
    super::task_adaptive::apply_adaptive_task_command(db, input).await
}
#[allow(unused_variables)]
pub(super) async fn insert_project_task_governance_in_tx(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    governance: CreateProjectTaskGovernance,
) -> Result<()> {
    super::task_adaptive::insert_task_governance_in_tx(transaction, &governance).await
}
pub(super) async fn create_task_proposal_command(
    db: &SqliteDb,
    input: CreateTaskProposalCommand,
) -> Result<CreatedTaskProposal> {
    let mut transaction = crate::begin_immediate(db.pool()).await?;
    let mut input = input;

    // Receipt lookup is deliberately the first operation.  A response
    // loss must replay the frozen Task even if the binding or baseline has
    // since moved on; changed input/principal is
    // rejected by the canonical receipt identity before any current
    // authorization read can produce a different answer.
    if let Some(receipt) =
        resolve_command_replay(db, &mut transaction, input.command_receipt.as_ref()).await?
    {
        let task_id = command_outcome_string(&receipt, "task_id")?;
        let task_snapshot = command_outcome_task(&receipt)?;
        if task_snapshot.id != task_id || task_snapshot.project_id != receipt.scope_id {
            return Err(DbError::IdempotencyConflict);
        }
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
    // command, recheck direct policy admission, target/scope, active
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

    if input.task.parent_task_id.as_ref().is_some_and(|parent_id| {
        input
            .depends_on_task_ids
            .iter()
            .any(|depends_on_task_id| depends_on_task_id == parent_id)
    }) {
        return Err(DbError::Check(
                "task proposal parent_task_id creates a shared-workspace subtask relationship and cannot also be a prerequisite dependency"
                    .to_owned(),
            ));
    }

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
        if governance.project_id != input.task.project_id || governance.task_id != input.task.id {
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
    let task = TaskRepo::create_in_tx(db, &mut transaction, create_task).await?;
    if let Some(metadata_json) = input.metadata_json.as_deref() {
        // This is create-time initialization: the Task was inserted in
        // this same transaction and is not visible to any independent
        // metadata writer until commit. Keep the caller's complete
        // initial document here; existing Tasks use `mutate_metadata`.
        sqlx::query(
            "UPDATE task SET metadata_json = ?, updated_at = ? WHERE id = ? AND project_id = ?",
        )
        .bind(metadata_json)
        .bind(&task.updated_at)
        .bind(&task.id)
        .bind(&task.project_id)
        .execute(&mut *transaction)
        .await?;
        crate::task_condition::produce(&mut transaction, &task.id, crate::ConditionChange::Legacy)
            .await?;
    }
    if !task.is_automation {
        ProjectRepo::increment_project_work_epoch(db, &mut transaction, &task.project_id, 1)
            .await?;
    }
    if let Some(governance) = input.governance {
        super::task_adaptive::insert_task_governance_in_tx(&mut transaction, &governance).await?;
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
    if !input.depends_on_task_ids.is_empty()
        && super::task_dependency::dependency_cycle_in_tx(&mut transaction, &task.id, None).await?
    {
        return Err(DbError::CycleDetected);
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
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;

    let mut receipt = input.command_receipt;
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
    }
    finalize_command_in_tx(db, &mut transaction, &event.id, receipt, None).await?;
    transaction.commit().await?;
    Ok(CreatedTaskProposal {
        task: frozen_task,
        replayed: false,
    })
}
