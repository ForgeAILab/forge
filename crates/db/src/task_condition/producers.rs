//! Producer seam: a writer states what its write changed and the condition is
//! rebuilt from the row in hand, the families that write can change and the
//! witnesses the stored condition already carries. Nothing else is read.
use super::facts::{Families, Snapshot};
use super::*;

/// The fact family a write can change. Every other family is carried from the
/// stored condition, so a budget charge reads no execution, a status write no
/// ledger, and a Task with neither parent nor children no other Task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionChange {
    /// The five legacy columns only.
    Legacy,
    /// Review or assignment changed the current human decision/work boundary.
    Human,
    /// Status, status epoch, an entry receipt, parentage or sibling order.
    /// Re-reads the entry, its hooks step and its execution binding, and
    /// refreshes the parent's child witnesses when the parent waits on them.
    Entry,
    /// An entry hooks step was enqueued, superseded or finished.
    Hooks,
    /// A running execution was admitted or ended, or its entry binding (its
    /// config snapshot) changed. Reads the Task's newest running execution
    /// from the running-only index, never its execution history. An execution
    /// inserted already settled changes no condition and needs no producer.
    Execution,
    /// A ledger charge or window reset.
    Budget,
    /// A remote cancellation was recorded, acknowledged or lost its workspace.
    Operations,
    /// The Task's visible children, their order or their settledness.
    Children,
    /// Everything: the mapping fallback for a writer with no narrower claim.
    Full,
}

/// A condition as one producer states it, with the fences it was read under.
pub(crate) struct Produced {
    pub version: i64,
    pub legacy: LegacyConditionInput,
    pub condition: TaskCondition,
    pub encoded: String,
    /// The stored text differs and must be written.
    pub changed: bool,
    /// A parent whose child witnesses this write may have changed.
    pub parent: Option<String>,
}

/// Build the condition after `change`. `metadata` replaces the row's
/// `metadata_json` for a writer that has not issued its `UPDATE` yet and will
/// fold the result into it.
pub(crate) async fn derive(
    c: &mut SqliteConnection,
    task_id: &str,
    change: ConditionChange,
    metadata: Option<Option<&str>>,
    stated: Option<&ConditionStatement>,
) -> Result<Option<Produced>> {
    use ConditionChange as Change;
    let families = match change {
        Change::Full | Change::Entry | Change::Human => Families::ENTRY,
        Change::Hooks => Families::HOOKS,
        Change::Execution => Families::EXECUTION,
        _ => Families::ROW,
    };
    let Some(mut snapshot) = Snapshot::read(c, task_id, families).await? else {
        return Ok(None);
    };
    let stored = match stored_condition(c, &snapshot.stored).await {
        Stored::Readable(stored) => Some(*stored),
        // Corrupt or empty: nothing can be carried from it. The mapping and
        // the durable facts restate the row below, statement or not.
        Stored::Corrupt(_) => None,
        // A newer build's encoding is never guessed at or overwritten. A
        // statement over it is refused whole, so its writer's transaction
        // (the legacy fields and the version bump) rolls back with it. A
        // legacy writer that states nothing still lands; its producer is
        // skipped and readers keep the typed unknown park.
        Stored::Newer => {
            if stated.is_some() {
                return Err(DbError::TaskConditionQuarantined {
                    task_id: task_id.to_owned(),
                });
            }
            tracing::debug!(task_id, "Task condition is quarantined; producer skipped");
            return Ok(None);
        }
    };
    if let Some(metadata) = metadata {
        snapshot.input.metadata_json = metadata.map(str::to_owned);
        snapshot
            .input
            .non_text
            .retain(|field| *field != LegacyConditionField::MetadataJson);
    }
    // A writer that states its condition edits the stored one. Only a stored
    // condition that is corrupt falls back to the mapping.
    let statement = stated;
    let stated = statement.and_then(|statement| Some(statement.apply(stored.as_ref()?)));
    let from_statement = stated.is_some();
    let mapped = stated.unwrap_or_else(|| map_view(&snapshot.input.view()));
    let exhausted = mapped.budget_exhausted();
    if change == Change::Budget && !exhausted {
        // Only an exhausted condition witnesses the ledger.
        return Ok(None);
    }
    let prior = (change != Change::Full)
        .then(|| ConditionFacts::recover_stored(&snapshot.stored))
        .flatten()
        // A status epoch the stored condition never saw: nothing is carried.
        .filter(|prior| change == Change::Entry || prior.epoch == snapshot.epoch);
    let pending = snapshot.children_pending();
    let mut facts = match prior {
        Some(mut facts) => {
            snapshot.refresh(c, &mut facts).await?;
            if change == Change::Operations {
                facts.load_operations(c).await?;
            }
            if !exhausted {
                facts.budgets.clear();
            } else if change == Change::Budget || facts.budgets.is_empty() {
                facts.load_budgets(c).await?;
            }
            facts.children_pending = pending;
            if !pending {
                facts.children.clear();
            } else if change == Change::Children || facts.children.is_empty() {
                facts.load_children(c, &snapshot.project).await?;
            }
            facts
        }
        None => {
            // The stored condition carries nothing usable: recompute it all.
            if families != Families::ENTRY {
                let Some(mut full) = Snapshot::read(c, task_id, Families::ENTRY).await? else {
                    return Ok(None);
                };
                full.input = std::mem::take(&mut snapshot.input);
                snapshot = full;
            }
            ConditionFacts::load_all(c, &snapshot).await?
        }
    };
    if let Some(statement) = statement {
        match statement {
            ConditionStatement::Integration { reason } => {
                // The attempt fence: an attempt restates its own reason. A
                // different attempt takes the row over only when no attempt
                // owns it: none was stated, the previous one was cleared, or
                // it was handed off to a role. A stale Task step holding an
                // older attempt cannot replace its successor's reason.
                if facts.integration.as_ref().is_some_and(|prior| {
                    prior.attempt_id() != reason.attempt_id() && !facts.integration_handoff_ready
                }) {
                    return Err(DbError::Check(
                        "another integration attempt owns this Task condition".into(),
                    ));
                }
                facts.integration_lineage = None;
                // Updating a delegated repair's paths keeps its role ready;
                // a new phase or attempt returns ownership to integration.
                facts.integration_handoff_ready &=
                    facts.integration.as_ref().is_some_and(|prior| {
                        prior.attempt_id() == reason.attempt_id()
                            && std::mem::discriminant(prior) == std::mem::discriminant(reason)
                    });
                facts.integration = Some(reason.clone());
            }
            ConditionStatement::IntegrationHandedOff { attempt_id } => {
                let reason = facts.integration.as_ref().ok_or(DbError::VersionConflict)?;
                if reason.attempt_id() != attempt_id {
                    return Err(DbError::VersionConflict);
                }
                if !reason.hands_off() {
                    return Err(DbError::Check(
                        "integration reason cannot hand off to a role".into(),
                    ));
                }
                facts.integration_handoff_ready = true;
            }
            ConditionStatement::IntegrationCleared { attempt_id } => {
                if facts
                    .integration
                    .as_ref()
                    .is_none_or(|reason| reason.attempt_id() != attempt_id)
                {
                    return Err(DbError::VersionConflict);
                }
                facts.integration = None;
                facts.integration_handoff_ready = false;
            }
            ConditionStatement::Check { wait, epoch } => {
                // Only the status entry that asked may state its wait.
                if *epoch != snapshot.epoch {
                    return Err(DbError::VersionConflict);
                }
                facts.check = Some((wait.clone(), *epoch));
            }
            ConditionStatement::CheckCleared { consumer_id } => {
                // A stale consumer never clears its successor's wait.
                if facts
                    .check
                    .as_ref()
                    .is_some_and(|(wait, _)| wait.consumer_id == *consumer_id)
                {
                    facts.check = None;
                }
            }
            _ => {}
        }
    }
    // An initial state reads the entry barrier differently; only then is the
    // mapping taken again, from the same snapshot.
    let condition = if (facts.initial && !from_statement)
        || matches!(
            statement,
            Some(
                ConditionStatement::IntegrationCleared { .. }
                    | ConditionStatement::CheckCleared { .. }
            )
        ) {
        // Clearing integration restores the other owners' current mapping,
        // including a Deferred timer whose continuation integration stood over.
        facts.condition_of(&snapshot.input.view())
    } else {
        facts.apply(mapped)
    };
    integration::validate(&condition)?;
    check::validate(&condition)?;
    let encoded = encode(&condition);
    Ok(Some(Produced {
        version: snapshot.version,
        changed: encoded.as_bytes() != snapshot.stored,
        parent: snapshot
            .parent_watches
            .then(|| snapshot.parent.clone())
            .flatten(),
        legacy: snapshot.input,
        condition,
        encoded,
    }))
}

/// The producer seam for a writer in its own transaction, after its write.
pub(crate) async fn produce(
    c: &mut SqliteConnection,
    task_id: &str,
    change: ConditionChange,
) -> Result<()> {
    let Some(produced) = derive(c, task_id, change, None, None).await? else {
        return Ok(());
    };
    if produced.changed {
        set_condition(c, task_id, &produced.encoded, Some(produced.version)).await?;
    }
    if let Some(parent) = &produced.parent {
        produce_children(c, parent).await?;
    }
    Ok(())
}

/// The seam for a writer that states its condition: same transaction, after
/// its write. The stored condition is edited by the statement and the durable
/// facts are laid over it; the fields the writer wrote are not mapped.
pub(crate) async fn state(
    c: &mut SqliteConnection,
    task_id: &str,
    statement: &ConditionStatement,
) -> Result<()> {
    if statement.owner_stated() {
        return Err(DbError::Check(
            "integration and check statements require state_integration_condition_in_tx".into(),
        ));
    }
    let Some(produced) = derive(c, task_id, ConditionChange::Legacy, None, Some(statement)).await?
    else {
        return Ok(());
    };
    if produced.changed {
        set_condition(c, task_id, &produced.encoded, Some(produced.version)).await?;
    }
    if let Some(parent) = &produced.parent {
        produce_children(c, parent).await?;
    }
    Ok(())
}

/// A child joined, left, moved within or settled under `parent_id`.
pub(crate) async fn produce_children(c: &mut SqliteConnection, parent_id: &str) -> Result<()> {
    if let Some(produced) = derive(c, parent_id, ConditionChange::Children, None, None).await? {
        if produced.changed {
            set_condition(c, parent_id, &produced.encoded, Some(produced.version)).await?;
        }
    }
    Ok(())
}

/// The Project's workflow was replaced. A Task's terminal classification and a
/// child's settledness read it, so restate exactly the Tasks whose status
/// classifies differently under the new definition, and every root that is
/// waiting on its children.
pub(crate) async fn workflow_changed(
    c: &mut SqliteConnection,
    project_id: &str,
    previous_workflow: &str,
) -> Result<()> {
    let workflow: String = sqlx::query_scalar("SELECT workflow_definition FROM project WHERE id=?")
        .bind(project_id)
        .fetch_one(&mut *c)
        .await?;
    if workflow == previous_workflow {
        return Ok(());
    }
    let (before, after) = (
        facts::Terminals::of(previous_workflow),
        facts::Terminals::of(&workflow),
    );
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT status FROM task WHERE project_id=?")
            .bind(project_id)
            .fetch_all(&mut *c)
            .await?;
    for status in statuses
        .iter()
        .filter(|status| before.differs(&after, status) || previous_workflow != workflow)
    {
        let tasks: Vec<String> =
            sqlx::query_scalar("SELECT id FROM task WHERE project_id=? AND status=?")
                .bind(project_id)
                .bind(status)
                .fetch_all(&mut *c)
                .await?;
        for task_id in &tasks {
            produce(c, task_id, ConditionChange::Entry).await?;
        }
    }
    let waiting: Vec<String> = sqlx::query_scalar("SELECT id FROM task WHERE project_id=? AND instr(COALESCE(metadata_json,''),'coordination_review_pending')>0")
        .bind(project_id)
        .fetch_all(&mut *c)
        .await?;
    for task_id in &waiting {
        produce_children(c, task_id).await?;
    }
    Ok(())
}

/// An execution was created in `workspace_id`. If a cancellation is still
/// unconfirmed there, the Task is now inside its fence.
pub(crate) async fn execution_joined_workspace(
    c: &mut SqliteConnection,
    task_id: &str,
    workspace_id: &str,
) -> Result<()> {
    let fenced: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pending_remote_cancel WHERE workspace_id=?)",
    )
    .bind(workspace_id)
    .fetch_one(&mut *c)
    .await?;
    if fenced {
        produce(c, task_id, ConditionChange::Operations).await?;
    }
    Ok(())
}

/// For a metadata writer that folds the shadow into its own `UPDATE`: the
/// text to store once `metadata_json` holds `metadata`, or `None` when the
/// stored condition already says it.
pub(crate) async fn metadata_condition(
    c: &mut SqliteConnection,
    task_id: &str,
    metadata: Option<&str>,
) -> Result<Option<String>> {
    Ok(
        derive(c, task_id, ConditionChange::Legacy, Some(metadata), None)
            .await?
            .filter(|produced| produced.changed)
            .map(|produced| produced.encoded),
    )
}

/// A producer whose write is authoritative on its own (a ledger charge, an
/// execution result, a step settlement, a cancellation marker): a failed
/// shadow write is logged and left to the invariant check.
pub(crate) async fn produce_best_effort(
    c: &mut SqliteConnection,
    task_id: &str,
    change: ConditionChange,
) {
    if let Err(error) = produce(c, task_id, change).await {
        tracing::warn!(task_id, ?change, %error, "Task condition not updated; the invariant check repairs it");
    }
}

/// The family a Task SQL statement can change, so the queue adapters skip the
/// seam for writes that cannot change a condition.
pub(crate) fn sql_change(query: &str) -> Option<ConditionChange> {
    let lower = query.to_ascii_lowercase();
    // A role or Review row moves the human decision/work boundary whatever
    // columns it names.
    let statement = lower.trim_start();
    if ["task_role_assignment", "review"].iter().any(|table| {
        [
            "update ",
            "delete from ",
            "insert into ",
            "insert or replace into ",
        ]
        .iter()
        .any(|verb| {
            statement
                .strip_prefix(verb)
                .and_then(|rest| rest.trim_start().strip_prefix(table))
                .is_some_and(|rest| !rest.starts_with(|c: char| c.is_alphanumeric() || c == '_'))
        })
    }) {
        return Some(ConditionChange::Human);
    }
    let assignments = lower.split(" where ").next().unwrap_or(&lower);
    let assigns = |columns: &[&str]| columns.iter().any(|column| assignments.contains(column));
    if assigns(&[
        "status",
        "status_epoch",
        "parent_task_id",
        "deleted_at",
        "subtask_order",
    ]) {
        Some(ConditionChange::Entry)
    } else if assigns(&[
        "error_annotation",
        "blocked_json",
        "failed_json",
        "entry_barrier_json",
        "metadata_json",
    ]) {
        Some(ConditionChange::Legacy)
    } else {
        None
    }
}
